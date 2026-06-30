## Push approved Gloucestershire hypothesis tracker findings to S3 and Redis.
##
## Source:  Monday.com board 18332582917, group "LA-ready"
## Targets:
##   s3://asc-analytics-dashboard-backend-development-data/gloucestershire/hypothesis_findings.json
##   Redis  gloucestershire:hypothesis_findings
##
## "LA-ready" group = approved findings ready for LA data product consumption.
## All items in this group are exported (Gloucestershire is the current active LA).
##
## Required env vars (same as other loaders):
##   BASTION_KEY_PATH, BASTION_HOST, BASTION_USER, REDIS_HOST, REDIS_PORT
##   AWS_DEFAULT_REGION (default eu-west-2); credentials from ~/.aws/dev
##
## Usage:
##   Rscript loaders/export_hypothesis_findings_to_s3.R

library(ascFuncs)
library(tidyverse)
library(logger)
library(cli)
library(jsonlite)
library(processx)

if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3", repos = "https://cloud.r-project.org")
if (!requireNamespace("redux",  quietly = TRUE)) install.packages("redux",  repos = "https://cloud.r-project.org")
library(redux)

# Config ------------------------------------------------------------------

HTS_BOARD_ID  <- "18332582917"
HTS_GROUP     <- "LA-ready"

S3_BUCKET  <- "asc-analytics-dashboard-backend-development-data"
S3_FOLDER  <- "gloucestershire"
S3_KEY     <- paste0(S3_FOLDER, "/hypothesis_findings.json")
REDIS_KEY  <- paste0(S3_FOLDER, ":hypothesis_findings")

BASTION_HOST <- Sys.getenv("BASTION_HOST", "ec2-3-9-19-63.eu-west-2.compute.amazonaws.com")
BASTION_USER <- Sys.getenv("BASTION_USER", "ec2-user")
BASTION_KEY  <- Sys.getenv("BASTION_KEY_PATH", "asc-social-care-analytics-dashboard-key-dev.pem")
REDIS_HOST   <- Sys.getenv("REDIS_HOST", "asc-analytics-dashboard-backend-redis-cache.eyzuby.0001.euw2.cache.amazonaws.com")
REDIS_PORT   <- as.integer(Sys.getenv("REDIS_PORT", "6379"))
TUNNEL_PORT  <- 6380L

if (nchar(BASTION_KEY) == 0) stop("BASTION_KEY_PATH env var not set.", call. = FALSE)

# ── Pull from Monday.com ──────────────────────────────────────────────────────

cli::cli_h1("Pulling hypothesis findings from Monday.com")
log_info("Board: {HTS_BOARD_ID} | Group: '{HTS_GROUP}'")

board <- ascFuncs::query_monday_board(HTS_BOARD_ID)

raw_df <- board$df
log_info("Board '{board$name}': {nrow(raw_df)} total rows, {length(unique(raw_df$group_title))} group(s)")

findings_df <- raw_df |>
  dplyr::filter(group_title == HTS_GROUP)

if (nrow(findings_df) == 0) {
  stop(
    "No items found in group '", HTS_GROUP, "'. ",
    "Check the group name hasn't been renamed on the board.",
    call. = FALSE
  )
}

log_info("{nrow(findings_df)} approved finding(s) in '{HTS_GROUP}'")

## Wrap with metadata for the consumer
output <- list(
  source     = "Monday.com Hypothesis Tracker",
  board_id   = HTS_BOARD_ID,
  group      = HTS_GROUP,
  pulled_at  = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  n_findings = nrow(findings_df),
  findings   = findings_df
)

json_str <- as.character(
  jsonlite::toJSON(output, auto_unbox = TRUE, null = "null", na = "null", pretty = TRUE)
)

# ── Phase 1: S3 ──────────────────────────────────────────────────────────────

cli::cli_h1("Phase 1: S3")

cred_file <- path.expand("~/.aws/dev")
if (!file.exists(cred_file)) stop("~/.aws/dev not found", call. = FALSE)
lines  <- readLines(cred_file)
key_id <- trimws(sub(".*=\\s*", "", grep("aws_access_key_id",     lines, value = TRUE, ignore.case = TRUE)[1]))
secret <- trimws(sub(".*=\\s*", "", grep("aws_secret_access_key", lines, value = TRUE, ignore.case = TRUE)[1]))
Sys.setenv(AWS_ACCESS_KEY_ID = key_id, AWS_SECRET_ACCESS_KEY = secret)
log_info("AWS credentials loaded from ~/.aws/dev")

tmp <- tempfile(fileext = ".json")
on.exit(unlink(tmp), add = TRUE)
writeLines(json_str, tmp)

aws.s3::put_object(
  file   = tmp,
  object = S3_KEY,
  bucket = S3_BUCKET,
  region = Sys.getenv("AWS_DEFAULT_REGION", "eu-west-2")
)
kb <- round(file.info(tmp)$size / 1024, 1)
log_info("{S3_KEY}: {kb} KB -> s3://{S3_BUCKET}/{S3_KEY}")

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
r$SET(REDIS_KEY, json_str)
log_info("Redis SET {REDIS_KEY}")

# ── Summary ──────────────────────────────────────────────────────────────────

cli::cli_h1("Done")
cli::cli_alert_success("S3:    s3://{S3_BUCKET}/{S3_KEY}")
cli::cli_alert_success("Redis: {REDIS_KEY}")
cli::cli_alert_info("{nrow(findings_df)} finding(s) from '{HTS_GROUP}' pushed")
