## Export all tables in ANALYTICS.PUBLIC_LA_PRODUCT to S3 (JSON) and Redis
##
## Source:  ANALYTICS.PUBLIC_LA_PRODUCT (all tables — discovered at runtime)
## Targets: s3://asc-analytics-dashboard-backend-development-data/gloucestershire/{TABLE}.json
##          Redis  gloucestershire:{table} (via SSH tunnel through bastion)
##
## Required env vars (Snowflake):
##   SNOWFLAKE_SERVER, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
##
## Required env vars (AWS / S3):
##   AWS_DEFAULT_REGION (default: eu-west-2)
##   Credentials loaded from ~/.aws/dev (313196411728:user/amitkohli)
##
## Required env vars (Redis / bastion):
##   BASTION_KEY_PATH  path to SSH private key (.pem)
##   BASTION_HOST      default: ec2-3-9-19-63.eu-west-2.compute.amazonaws.com
##   BASTION_USER      default: ec2-user
##   REDIS_HOST        default: asc-analytics-dashboard-backend-redis-cache.eyzuby.0001.euw2.cache.amazonaws.com
##   REDIS_PORT        default: 6379
##
## Usage:
##   Rscript loaders/export_la_queries_to_s3.R

library(ascFuncs)
library(tidyverse)
library(logger)
library(cli)
library(jsonlite)
library(processx)

if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3", repos = "https://cloud.r-project.org")
if (!requireNamespace("redux", quietly = TRUE)) install.packages("redux", repos = "https://cloud.r-project.org")
library(redux)

# Config ------------------------------------------------------------------

SOURCE_DB     <- "ANALYTICS"
SOURCE_SCHEMA <- "PUBLIC_LA_PRODUCT"
S3_FOLDER     <- "gloucestershire"

S3_BUCKET <- "asc-analytics-dashboard-backend-development-data"

BASTION_HOST <- Sys.getenv("BASTION_HOST", "ec2-3-9-19-63.eu-west-2.compute.amazonaws.com")
BASTION_USER <- Sys.getenv("BASTION_USER", "ec2-user")
BASTION_KEY  <- Sys.getenv("BASTION_KEY_PATH", "asc-social-care-analytics-dashboard-key-dev.pem")
REDIS_HOST   <- Sys.getenv("REDIS_HOST", "asc-analytics-dashboard-backend-redis-cache.eyzuby.0001.euw2.cache.amazonaws.com")
REDIS_PORT   <- as.integer(Sys.getenv("REDIS_PORT", "6379"))
TUNNEL_PORT  <- 6380L

if (nchar(BASTION_KEY) == 0) {
  stop("BASTION_KEY_PATH env var is not set.", call. = FALSE)
}

# ── Phase 1: S3 ──────────────────────────────────────────────────────────────

cli::cli_h1("Phase 1: Snowflake -> S3")

log_info("Target bucket: s3://{S3_BUCKET}/{S3_FOLDER}/")

log_info("Connecting to {SOURCE_DB}.{SOURCE_SCHEMA}")
con <- ascFuncs::connect_snowflake(database = SOURCE_DB)
on.exit(DBI::dbDisconnect(con), add = TRUE)

# Discover tables via INFORMATION_SCHEMA — dbListTables() invalidates the ODBC pointer
tables <- DBI::dbGetQuery(con, glue::glue(
  "SELECT TABLE_NAME FROM {SOURCE_DB}.INFORMATION_SCHEMA.TABLES ",
  "WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_TYPE = 'BASE TABLE' ",
  "ORDER BY TABLE_NAME"
))
table_names <- tables$TABLE_NAME

if (length(table_names) == 0) {
  stop(glue::glue("No tables found in {SOURCE_DB}.{SOURCE_SCHEMA}."), call. = FALSE)
}

log_info("Found {length(table_names)} tables: {paste(table_names, collapse = ', ')}")

# Fetch all tables once, cache for Redis phase
data_cache <- list()

cli::cli_h2("Fetching tables from Snowflake")
purrr::walk(table_names, function(table_name) {
  df <- tryCatch(
    ascFuncs::snowflake_read_table(con, table_name = table_name, schema = SOURCE_SCHEMA, database = SOURCE_DB),
    error = function(e) {
      log_error("Query failed for {table_name}: {e$message}")
      NULL
    }
  )
  if (!is.null(df) && nrow(df) > 0) {
    log_info("{table_name}: {nrow(df)} rows fetched")
    data_cache[[table_name]] <<- df
  } else {
    log_warn("{table_name}: 0 rows — skipping")
  }
})

# Load dev credentials from ~/.aws/dev
cred_file <- path.expand("~/.aws/dev")
if (!file.exists(cred_file)) stop("~/.aws/dev not found", call. = FALSE)
lines  <- readLines(cred_file)
key_id <- trimws(sub(".*=\\s*", "", grep("aws_access_key_id",     lines, value = TRUE, ignore.case = TRUE)[1]))
secret <- trimws(sub(".*=\\s*", "", grep("aws_secret_access_key", lines, value = TRUE, ignore.case = TRUE)[1]))
Sys.setenv(AWS_ACCESS_KEY_ID = key_id, AWS_SECRET_ACCESS_KEY = secret)
log_info("AWS credentials loaded from ~/.aws/dev (key: {key_id})")

all_failed <- character(0)

purrr::iwalk(data_cache, function(df, table_name) {
  json_path <- tempfile(fileext = ".json")
  on.exit(unlink(json_path), add = TRUE)
  jsonlite::write_json(df, json_path, auto_unbox = TRUE, null = "null", na = "null")

  result <- tryCatch(
    {
      aws.s3::put_object(
        file   = json_path,
        object = paste0(S3_FOLDER, "/", table_name, ".json"),
        bucket = S3_BUCKET,
        region = Sys.getenv("AWS_DEFAULT_REGION", "eu-west-2")
      )
      kb <- round(file.info(json_path)$size / 1024, 1)
      log_info("{table_name}: {kb} KB -> s3://{S3_BUCKET}/{S3_FOLDER}/{table_name}.json")
      "ok"
    },
    error = function(e) {
      log_error("S3 upload failed for {table_name}: {e$message}")
      "failed"
    }
  )
  if (result == "failed") all_failed <<- c(all_failed, table_name)
})

if (length(all_failed) > 0) {
  stop(glue::glue("S3 failures: {paste(all_failed, collapse = ', ')}"), call. = FALSE)
}

log_info("S3 complete: {length(data_cache)} tables -> s3://{S3_BUCKET}/{S3_FOLDER}/")

# ── Phase 2: Redis ───────────────────────────────────────────────────────────

cli::cli_h1("Phase 2: Redis (via SSH tunnel)")

log_info(
  "Opening SSH tunnel: localhost:{TUNNEL_PORT} -> {REDIS_HOST}:{REDIS_PORT} ",
  "via {BASTION_USER}@{BASTION_HOST}"
)

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
  stderr = "|"
)
on.exit({ if (ssh_proc$is_alive()) ssh_proc$kill() }, add = TRUE)

Sys.sleep(3)

if (!ssh_proc$is_alive()) {
  err <- ssh_proc$read_error()
  stop(glue::glue("SSH tunnel failed to start: {err}"), call. = FALSE)
}

log_info("SSH tunnel established (PID: {ssh_proc$get_pid()})")

r <- redux::hiredis(host = "127.0.0.1", port = TUNNEL_PORT)
log_info("Connected to Redis on localhost:{TUNNEL_PORT}")

redis_results <- purrr::imap(data_cache, function(df, table_name) {
  key <- paste0(S3_FOLDER, ":", tolower(table_name))
  json_str <- as.character(jsonlite::toJSON(df, auto_unbox = TRUE, null = "null", na = "null"))

  tryCatch(
    {
      r$SET(key, json_str)
      log_info("Redis SET {key} ({nrow(df)} rows)")
      list(table = table_name, status = "ok", rows = nrow(df))
    },
    error = function(e) {
      log_error("Redis SET failed for {table_name}: {e$message}")
      list(table = table_name, status = "failed", rows = nrow(df))
    }
  )
})

redis_summary <- purrr::map_dfr(redis_results, ~as.data.frame(.x))
n_redis_ok     <- sum(redis_summary$status == "ok")
n_redis_failed <- sum(redis_summary$status == "failed")
log_info("Redis complete: {n_redis_ok} keys set, {n_redis_failed} failed")

# ── Final summary ────────────────────────────────────────────────────────────

cli::cli_h1("Done")
cli::cli_alert_success("S3:    {length(data_cache)} tables -> s3://{S3_BUCKET}/{S3_FOLDER}/")
cli::cli_alert_success("Redis: {n_redis_ok} keys set (gloucestershire:<table>)")

if (n_redis_failed > 0) {
  stop(glue::glue("Redis failures: {paste(redis_summary$table[redis_summary$status == 'failed'], collapse = ', ')}"), call. = FALSE)
}
