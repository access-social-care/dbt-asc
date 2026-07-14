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
##   NO_UPDATE  -> load ONLY if the target Snowflake table doesn't exist yet
##                 (first-time load using the still-valid on-disk CSV);
##                 skip if the table already exists (nothing to reload).
##                 NO_UPDATE means "the SOURCE hasn't changed since last
##                 extraction" - a completely different question from "has
##                 this ever been loaded to Snowflake," which bit on the very
##                 first loader run (every dataset came back NO_UPDATE against
##                 a valid on-disk CSV, and all 10 were skipped for no reason).
##   LIMITATION -> skip (documented gap, e.g. discharge_delays — never loaded)
##   FAILED_*   -> skip + warn loudly; script exits non-zero if any occurred
##                 (cc picks this up like any other pipeline failure)
##
## Every dataset's row also gets a _RUN_ID, _RUN_AT and _DRIFT_FLAG column
## (from manifest provenance) so drift warnings survive past the CLI/console
## output that scrolls away. Publication date is NOT added here - the CSV
## already carries `_publication_date` from source_checker's own tagging.
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

## Default folder name below is the actual on-disk sibling checkout
## (amit_claude_data_firecrawl) - the repo has NOT been renamed/re-cloned
## under external_source_freshness_checker on disk yet, only on GitHub
## (see fork note above). Confirmed 2026-07-14: the previous default here
## pointed at a folder that doesn't exist, which meant this loader had
## never successfully run in prod and REFERENCE.PUBLIC.PEOPLE_RECEIVING_CARE
## (and every other data_portal table) was never created - silent no-op,
## not an error, until dbt's source test hit the missing table.
SOURCE_DIR <- Sys.getenv(
  "DATA_PORTAL_SOURCE_DIR",
  normalizePath(
    file.path(dirname(getwd()), "amit_claude_data_firecrawl", "data"),
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

## Raw DBI call, not a hand-rolled anti-pattern here — ascFuncs exports no
## session-info helper, and this is the exact pattern used identically in
## load_advicepro_demographics_to_snowflake.R, load_casework_locality_to_
## snowflake.R and load_member_orgs_to_snowflake.R (confirmed by grep across
## loaders/*.R). Matched to that convention here, including CURRENT_USER().
session_info <- DBI::dbGetQuery(
  con,
  "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER()"
)
log_info(
  "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
  "database={session_info[[1, 'CURRENT_DATABASE()']]} ",
  "user={session_info[[1, 'CURRENT_USER()']]}"
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

## Mirrors the existence check inside ascFuncs::snowflake_write_table - but
## that check is a local variable inside the function body, not exported or
## callable on its own (confirmed by reading ascFuncs source, 2026-07-14:
## ascFuncs has no exported table-exists/session-info helper at all). This
## loader needs the answer standalone, before calling snowflake_write_table,
## to decide first-time-load logging - so the duplication here is
## unavoidable given ascFuncs's current public API, not a shortcut taken in
## place of an available helper.
table_exists <- function(con, database, schema, table_name) {
  full_name <- paste(database, schema, toupper(table_name), sep = ".")
  tryCatch(
    expr  = {
      DBI::dbGetQuery(conn = con, statement = paste0(
        "SELECT 1 FROM ", full_name, " LIMIT 0"
      ))
      TRUE
    },
    error = function(e) FALSE
  )
}

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
  if (grepl("^FAILED_", status)) {
    log_warn("{id}: {status} — {dataset$message %||% 'no message'} — NOT loaded")
    n_failed <- n_failed + 1L
    next
  }
  first_time_load <- FALSE
  if (status == "NO_UPDATE") {
    if (table_exists(con, TARGET_DB, "PUBLIC", id)) {
      log_info("{id}: NO_UPDATE — table already current, skipping")
      n_skipped <- n_skipped + 1L
      next
    }
    log_info("{id}: NO_UPDATE from source, but table doesn't exist in Snowflake yet — first-time load")
    first_time_load <- TRUE
  } else if (status != "SUCCESS") {
    log_warn("{id}: unrecognised status '{status}' — skipping defensively")
    n_skipped <- n_skipped + 1L
    next
  }

  csv_path <- file.path(SOURCE_DIR, paste0(id, ".csv"))
  if (!file.exists(csv_path)) {
    log_warn("{id}: {status} in manifest but {csv_path} missing — skipping")
    n_failed <- n_failed + 1L
    next
  }

  df <- readr::read_csv(csv_path, show_col_types = FALSE)
  # NOTE: does NOT add a publication-date column here - source_checker's own
  # tagging.py already writes one (`_publication_date`, same value, same
  # source: resolved.inferred_publication_date / last_known_good). Adding a
  # second one collided case-insensitively once Snowflake uppercases every
  # column name ("duplicate column name '_PUBLICATION_DATE'").
  df$`_RUN_ID` <- manifest$run_id
  df$`_RUN_AT` <- manifest$run_at
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
  load_kind <- if (first_time_load) "first-time load" else "loaded"
  log_info("{id}: {load_kind}, {nrow(df)} rows -> {TARGET_DB}.PUBLIC.{toupper(id)}")
  n_loaded <- n_loaded + 1L
}

cli::cli_h2("Summary")
cli::cli_alert_success("{n_loaded} loaded, {n_skipped} skipped, {n_failed} failed")

if (n_failed > 0) {
  stop(n_failed, " dataset(s) failed to load — see warnings above.", call. = FALSE)
}
