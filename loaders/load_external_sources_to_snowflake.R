## Load the external data-portal CSVs (ASC benchmarking statistics) to Snowflake
##
## Source: external_source_freshness_checker (extract → verify → CSV + manifest.json)
##   repo: access-social-care/external_source_freshness_checker
##   (fork of mpr3z1v3/amit_claude_data_firecrawl — asc-agent)
## Target: REFERENCE.PUBLIC.<dataset_id> (one table per registry dataset)
##
## This script does ONLY connect -> write -> grant. All shape/column
## verification already happened in asc-agent's own verifier (never
## re-validated here — see data-engineer agent notes on not duplicating the
## extraction repo's logic in the loader).
##
## Per-dataset status from manifest.json drives the action:
##   SUCCESS    -> load (data changed this run)
##   NO_UPDATE  -> skip (table is already current, nothing to reload)
##   LIMITATION -> skip (documented gap, e.g. discharge_delays — never loaded)
##   FAILED_*   -> skip + warn loudly; script exits non-zero if any occurred
##                 (cc picks this up like any other pipeline failure)
##
## Every dataset's row also gets a _RUN_ID, _RUN_AT, _PUBLICATION_DATE and
## _DRIFT_FLAG column (from manifest provenance) so drift warnings survive
## past the CLI/console output that scrolls away.
##
## Usage:
##   Rscript loaders/load_external_sources_to_snowflake.R
##   (run from dbt-asc/ root, or via absolute path from cron)
##
## SOURCE_DIR defaults to a sibling checkout; override via env var if the
## extraction repo lives elsewhere (e.g. a different path on the VM).

library(ascFuncs)
library(tidyverse)
library(jsonlite)
library(logger)
library(cli)

# Config --------------------------------------------------------------------

SOURCE_DIR <- Sys.getenv(
  "DATA_PORTAL_SOURCE_DIR",
  normalizePath(
    file.path(dirname(getwd()), "external_source_freshness_checker", "data"),
    mustWork = FALSE
  )
)
TARGET_DB <- "REFERENCE"
MANIFEST_PATH <- file.path(SOURCE_DIR, "manifest.json")

cli::cli_h1("Loading data-portal CSVs to Snowflake")
cli::cli_alert_info("Source: {SOURCE_DIR}")

if (!file.exists(MANIFEST_PATH)) {
  stop("manifest.json not found at ", MANIFEST_PATH, " — run asc-agent first.", call. = FALSE)
}

manifest <- jsonlite::fromJSON(MANIFEST_PATH, simplifyDataFrame = FALSE)
log_info("Manifest run_id={manifest$run_id} run_at={manifest$run_at}")

# Connect once, reuse for every table ----------------------------------------

con <- ascFuncs::connect_snowflake(database = TARGET_DB, role = NULL)
on.exit(DBI::dbDisconnect(con), add = TRUE)

session_info <- DBI::dbGetQuery(
  con,
  "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
)
log_info(
  "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
  "database={session_info[[1, 'CURRENT_DATABASE()']]}"
)

# Per-dataset provenance helper -----------------------------------------------

## Drift codes are recorded per-source in provenance$warnings (a list of
## {code, message, details}); a dataset can have >1 source (multi-file merge).
extract_drift_flag <- function(dataset) {
  sources <- dataset$provenance$sources
  all_warnings <- dataset$provenance$warnings
  codes <- character(0)
  if (!is.null(all_warnings)) {
    codes <- c(codes, vapply(all_warnings, function(w) w$code %||% "", character(1)))
  }
  drift_codes <- unique(codes[codes %in% c("SCHEMA_DRIFT", "SEMANTIC_DRIFT_SUSPECTED")])
  if (length(drift_codes) == 0) NA_character_ else paste(drift_codes, collapse = "+")
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# Load each dataset ------------------------------------------------------------

n_loaded <- 0L
n_skipped <- 0L
n_failed <- 0L

for (dataset in manifest$datasets) {
  id <- dataset$dataset_id
  status <- dataset$status

  if (status == "LIMITATION") {
    log_info("{id}: LIMITATION — not loaded (documented gap)")
    n_skipped <- n_skipped + 1L
    next
  }
  if (status == "NO_UPDATE") {
    log_info("{id}: NO_UPDATE — table already current, skipping")
    n_skipped <- n_skipped + 1L
    next
  }
  if (grepl("^FAILED_", status)) {
    log_warn("{id}: {status} — {dataset$message %||% 'no message'} — NOT loaded")
    n_failed <- n_failed + 1L
    next
  }
  if (status != "SUCCESS") {
    log_warn("{id}: unrecognised status '{status}' — skipping defensively")
    n_skipped <- n_skipped + 1L
    next
  }

  csv_path <- file.path(SOURCE_DIR, paste0(id, ".csv"))
  if (!file.exists(csv_path)) {
    log_warn("{id}: SUCCESS in manifest but {csv_path} missing — skipping")
    n_failed <- n_failed + 1L
    next
  }

  df <- readr::read_csv(csv_path, show_col_types = FALSE)
  df$`_RUN_ID` <- manifest$run_id
  df$`_RUN_AT` <- manifest$run_at
  df$`_PUBLICATION_DATE` <- dataset$publication_date %||% NA_character_
  df$`_DRIFT_FLAG` <- extract_drift_flag(dataset)

  drift_flag <- extract_drift_flag(dataset)
  if (!is.na(drift_flag)) {
    cli::cli_alert_warning("{id}: drift flagged this run — {drift_flag} (see manifest for detail)")
  }

  ascFuncs::snowflake_write_table(
    con        = con,
    table_name = id,
    data       = df,
    database   = TARGET_DB,
    schema     = "PUBLIC",
    overwrite  = TRUE
  )
  log_info("{id}: loaded {nrow(df)} rows -> {TARGET_DB}.PUBLIC.{toupper(id)}")
  n_loaded <- n_loaded + 1L
}

cli::cli_h2("Summary")
cli::cli_alert_success("{n_loaded} loaded, {n_skipped} skipped, {n_failed} failed")

if (n_failed > 0) {
  stop(n_failed, " dataset(s) failed to load — see warnings above.", call. = FALSE)
}
