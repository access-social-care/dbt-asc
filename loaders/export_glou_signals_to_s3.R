## Push Gloucestershire signal outputs to S3 and Redis.
##
## Sources:
##   signal_processing/output/la_signals_narrative_*.json  (latest by date suffix)
##   signal_processing/output/la_peer_signals_*.csv        (latest, filtered to Gloucestershire)
##
## Targets:
##   s3://asc-analytics-dashboard-backend-development-data/gloucestershire/signals_narrative.json
##   s3://asc-analytics-dashboard-backend-development-data/gloucestershire/signals_glou.json
##   Redis  gloucestershire:signals_narrative
##   Redis  gloucestershire:signals_glou
##
## Required env vars: same as export_la_queries_to_s3.R
##   BASTION_KEY_PATH, BASTION_HOST, BASTION_USER, REDIS_HOST, REDIS_PORT
##   AWS_DEFAULT_REGION (default eu-west-2); credentials from ~/.aws/dev
##   OPENAI_API_KEY, LANGCHAIN_API_KEY, LANGCHAIN_TRACING_V2=true (NLG step)
##   SIGNAL_PROCESSING_DIR  (path to signal_processing repo, for NLG script)
##
## Usage:
##   Rscript loaders/export_glou_signals_to_s3.R

library(tidyverse)
library(logger)
library(cli)
library(jsonlite)
library(processx)

if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3", repos = "https://cloud.r-project.org")
if (!requireNamespace("redux",  quietly = TRUE)) install.packages("redux",  repos = "https://cloud.r-project.org")
library(redux)

# Config ------------------------------------------------------------------

SIGNAL_OUTPUT_DIR <- Sys.getenv(
  "SIGNAL_OUTPUT_DIR",
  "/srv/projects/signal-processing/output"
)

S3_BUCKET  <- "asc-analytics-dashboard-backend-development-data"
S3_FOLDER  <- "gloucestershire"

BASTION_HOST <- Sys.getenv("BASTION_HOST", "ec2-3-9-19-63.eu-west-2.compute.amazonaws.com")
BASTION_USER <- Sys.getenv("BASTION_USER", "ec2-user")
BASTION_KEY  <- Sys.getenv("BASTION_KEY_PATH", "asc-social-care-analytics-dashboard-key-dev.pem")
REDIS_HOST   <- Sys.getenv("REDIS_HOST", "asc-analytics-dashboard-backend-redis-cache.eyzuby.0001.euw2.cache.amazonaws.com")
REDIS_PORT   <- as.integer(Sys.getenv("REDIS_PORT", "6379"))
TUNNEL_PORT  <- 6380L

if (nchar(BASTION_KEY) == 0) stop("BASTION_KEY_PATH env var not set.", call. = FALSE)

LA_NAME <- "Gloucestershire"

SIGNAL_PROCESSING_DIR <- Sys.getenv(
  "SIGNAL_PROCESSING_DIR",
  "/srv/projects/signal-processing"
)

# ── Step 0: NLG — generate prose narrative ───────────────────────────────────

cli::cli_h1("Step 0: NLG")

nlg_script  <- file.path(SIGNAL_PROCESSING_DIR, "nlg", "signal_to_narrative.py")
nlg_venv_py <- file.path(SIGNAL_PROCESSING_DIR, "nlg", ".venv", "bin", "python")
prose_file  <- file.path(SIGNAL_OUTPUT_DIR, paste0("la_signals_prose_", Sys.Date(), ".json"))

if (!file.exists(nlg_script)) {
  log_warn("NLG script not found at {nlg_script} — skipping prose generation")
} else if (!file.exists(nlg_venv_py)) {
  stop("NLG venv not found at {nlg_venv_py}. Run: bash nlg/setup_venv.sh", call. = FALSE)
} else {
  log_info("Running NLG via venv: {nlg_venv_py}")
  nlg_result <- processx::run(
    nlg_venv_py,
    args = c(nlg_script, "--la", LA_NAME, "--out", prose_file),
    echo = TRUE,
    error_on_status = FALSE
  )
  if (nlg_result$status != 0) {
    log_warn("NLG step exited {nlg_result$status} — prose will be omitted from payload")
    prose_file <- NULL
  } else {
    log_info("Prose written to {prose_file}")
  }
}

# ── Read latest signal outputs ────────────────────────────────────────────────

cli::cli_h1("Reading signal outputs")

## Latest narrative JSON
narrative_files <- list.files(SIGNAL_OUTPUT_DIR, pattern = "^la_signals_narrative_.*\\.json$",
                               full.names = TRUE)
if (length(narrative_files) == 0) stop("No la_signals_narrative_*.json found in ", SIGNAL_OUTPUT_DIR)
narrative_file <- sort(narrative_files) |> tail(1)
log_info("Narrative: {narrative_file}")

narrative_json_full <- jsonlite::read_json(narrative_file)
## Extract Gloucestershire only
la_narrative <- narrative_json_full$las[[LA_NAME]]
if (is.null(la_narrative)) {
  log_warn("No '{LA_NAME}' entry in narrative JSON - check that the PoC script has been re-run")
  la_narrative <- list()
}
glou_narrative <- list(
  run_date = narrative_json_full$run_date,
  la       = LA_NAME,
  prose    = if (!is.null(prose_file) && file.exists(prose_file))
               jsonlite::read_json(prose_file)$prose
             else NULL,
  data     = la_narrative
)
log_info("Narrative signals for {LA_NAME}: {la_narrative$total_signals %||% 0}")

## Latest signals CSV
signal_files <- list.files(SIGNAL_OUTPUT_DIR, pattern = "^la_peer_signals_.*\\.csv$",
                            full.names = TRUE)
if (length(signal_files) == 0) stop("No la_peer_signals_*.csv found in ", SIGNAL_OUTPUT_DIR)
signal_file <- sort(signal_files) |> tail(1)
log_info("Signals CSV: {signal_file}")

glou_signals_df <- readr::read_csv(signal_file, show_col_types = FALSE) |>
  dplyr::filter(unit == LA_NAME)
log_info("{nrow(glou_signals_df)} {LA_NAME} signal rows loaded")

if (nrow(glou_signals_df) == 0) {
  log_warn("No signals found for {LA_NAME} in {signal_file}")
}

# ── Phase 1: S3 ──────────────────────────────────────────────────────────────

cli::cli_h1("Phase 1: S3")

cred_file <- path.expand("~/.aws/dev")
if (!file.exists(cred_file)) stop("~/.aws/dev not found", call. = FALSE)
lines  <- readLines(cred_file)
key_id <- trimws(sub(".*=\\s*", "", grep("aws_access_key_id",     lines, value = TRUE, ignore.case = TRUE)[1]))
secret <- trimws(sub(".*=\\s*", "", grep("aws_secret_access_key", lines, value = TRUE, ignore.case = TRUE)[1]))
Sys.setenv(AWS_ACCESS_KEY_ID = key_id, AWS_SECRET_ACCESS_KEY = secret)
log_info("AWS credentials loaded from ~/.aws/dev")

s3_push <- function(obj_json, s3_key) {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)
  writeLines(obj_json, tmp)
  aws.s3::put_object(
    file   = tmp,
    object = s3_key,
    bucket = S3_BUCKET,
    region = Sys.getenv("AWS_DEFAULT_REGION", "eu-west-2")
  )
  kb <- round(file.info(tmp)$size / 1024, 1)
  log_info("{s3_key}: {kb} KB -> s3://{S3_BUCKET}/{s3_key}")
}

narrative_out <- as.character(jsonlite::toJSON(glou_narrative,    auto_unbox = TRUE, null = "null", na = "null", pretty = TRUE))
signals_out   <- as.character(jsonlite::toJSON(glou_signals_df,   auto_unbox = TRUE, null = "null", na = "null"))

s3_push(narrative_out, paste0(S3_FOLDER, "/signals_narrative.json"))
s3_push(signals_out,   paste0(S3_FOLDER, "/signals_glou.json"))

# ── Phase 2: Redis ───────────────────────────────────────────────────────────

cli::cli_h1("Phase 2: Redis")

log_info("Opening SSH tunnel -> {REDIS_HOST}:{REDIS_PORT} via {BASTION_USER}@{BASTION_HOST}")
ssh_proc <- processx::process$new(
  "ssh",
  args = c(
    "-N",
    "-L", paste0(TUNNEL_PORT, ":", REDIS_HOST, ":", REDIS_PORT),
    "-i", BASTION_KEY,
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    paste0(BASTION_USER, "@", BASTION_HOST)
  ),
  supervise = TRUE,
  stderr    = "|"
)
on.exit({ if (ssh_proc$is_alive()) ssh_proc$kill() }, add = TRUE)

Sys.sleep(3)
if (!ssh_proc$is_alive()) {
  stop(glue::glue("SSH tunnel failed: {ssh_proc$read_error()}"), call. = FALSE)
}
log_info("Tunnel up (PID {ssh_proc$get_pid()})")

r <- redux::hiredis(host = "127.0.0.1", port = TUNNEL_PORT)

r$SET(paste0(S3_FOLDER, ":signals_narrative"), narrative_out)
log_info("Redis SET {S3_FOLDER}:signals_narrative")

r$SET(paste0(S3_FOLDER, ":signals_glou"), signals_out)
log_info("Redis SET {S3_FOLDER}:signals_glou")

# ── Summary ──────────────────────────────────────────────────────────────────

cli::cli_h1("Done")
cli::cli_alert_success("S3:    gloucestershire/signals_narrative.json + signals_glou.json")
cli::cli_alert_success("Redis: gloucestershire:signals_narrative + gloucestershire:signals_glou")
cli::cli_alert_info("Signals: {nrow(glou_signals_df)} rows | Narrative topics: {length(la_narrative$topics %||% list())}")
