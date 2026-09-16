## Load the external data-portal CSVs (ASC benchmarking statistics) to Snowflake
##
## Source: external_source_freshness_checker
##   (extract → verify → CSV + manifest.json)
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
##   run_pipeline.sh's run_loader() cd's into dbt-asc/loaders/ before calling
##   this, so DATA_PORTAL_SOURCE_DIR below is NOT a sibling of this script's
##   own cwd at runtime — see run_pipeline.sh, which sets the env var
##   explicitly to Stage 0's real output dir (EXTRACTOR_DIR/data) rather than
##   relying on the fallback default below.
##
## SOURCE_DIR: override via DATA_PORTAL_SOURCE_DIR env var (run_pipeline.sh
## always does). The hardcoded fallback below is only for manual/ad-hoc runs
## outside the pipeline and is frequently wrong — verify it before trusting
## it (confirmed 2026-07-24: it pointed at dbt-asc/amit_claude_data_
## firecrawl/data, a folder that has never existed on the VM).

library(ascFuncs)
library(tidyverse)
library(jsonlite)
library(logger)
library(cli)

# Config --------------------------------------------------------------------

## Confirmed 2026-07-24: the extraction repo WAS renamed/re-cloned on disk to
## external_source_freshness_checker (see EXTRACTOR_DIR in run_pipeline.sh) -
## a prior version of this comment claimed otherwise. Fallback below assumes
## a sibling checkout of dbt-asc/ itself (not dbt-asc/loaders/) under that
## name; still only correct for manual runs from dbt-asc/ root with that
## exact layout - prefer setting DATA_PORTAL_SOURCE_DIR explicitly.
SOURCE_DIR <- Sys.getenv(
  "DATA_PORTAL_SOURCE_DIR",
  normalizePath(
    file.path(dirname(getwd()), "external_source_freshness_checker", "data"),
    mustWork = FALSE
  )
)
TARGET_DB <- "REFERENCE"
MANIFEST_PATH <- file.path(SOURCE_DIR, "manifest.json")

# Landing (append-mode) datasets ----------------------------------------------
#
# Most datasets in this loader are a full-replace ("overwrite"): the CSV is
# the whole current truth and history is not retained. The CLD quarterly
# series is different. Each quarterly release republishes a rolling window of
# months, and an LA-month present in an older vintage can be ABSENT from a
# newer one. Overwriting would silently drop those months. So these datasets
# ACCUMULATE: every vintage is appended to a landing table and the dbt
# staging models (models/staging/external/stg_cld_*.sql) pick the newest
# vintage per LA-month with a window function.
#
# Location: EXTERNAL_DATA.LANDING (a dedicated database, not a schema tucked
# inside REFERENCE - REFERENCE is curated denominator/reference data, this is
# raw landed source data, and the two should never share a database). Needs
# EXTERNAL_DATA + its schemas created and granted to this loader's role
# BEFORE this runs - see loaders/sql/create_external_data_database.sql, a
# one-time script for whoever holds SYSADMIN to run. This loader does not and
# should not attempt to create the database itself.
LANDING_DB <- "EXTERNAL_DATA"
LANDING_SCHEMA <- "LANDING"

## Registry-driven would be cleaner still, but the manifest this loader reads
## does not carry per-dataset target overrides today, and inventing a
## `snowflake_target:` key in the checker's registry.yaml would mean shipping
## a coordinated change to a second repo before this one can run at all. The
## override therefore lives here as an explicit, greppable allow-list keyed by
## dataset_id. Adding a dataset to this vector is the ONLY way to get
## append/landing behaviour - every other dataset keeps its existing
## overwrite-to-REFERENCE.PUBLIC behaviour untouched.
LANDING_DATASETS <- c("cld_long_term_support", "cld_assessments")

cli::cli_h1("Loading data-portal CSVs to Snowflake")
cli::cli_alert_info("Source: {SOURCE_DIR}")

if (!file.exists(MANIFEST_PATH)) {
  stop(
    "manifest.json not found at ", MANIFEST_PATH, " — run asc-agent first.",
    call. = FALSE
  )
}

manifest <- jsonlite::fromJSON(MANIFEST_PATH, simplifyDataFrame = FALSE)
log_info("Manifest run_id={manifest$run_id} run_at={manifest$run_at}")

# Connect once, reuse for every table ----------------------------------------

con <- ascFuncs::connect_snowflake(database = TARGET_DB, role = NULL)
# on.exit(DBI::dbDisconnect(con), add = TRUE)

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

## extract_drift_flag() and %||% now live in loaders/lib/extract_drift_flag.R
## so they can be unit-tested without a Snowflake connection (see tests/
## testthat/test-extract_drift_flag.R).
source("lib/extract_drift_flag.R")

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

# Landing helpers --------------------------------------------------------------

## The CLD tidy output carries source column headers verbatim, and several of
## them contain spaces ("Support setting", "Age group", "LA code", "Area
## code", "Area unit"). snowflake_write_table() uppercases names but does not
## de-space them, and DBI quotes identifiers on create - so those would land
## as quoted identifiers ("SUPPORT SETTING") that every downstream dbt model
## would then have to quote too, against this repo's standing no-quoted-
## identifiers rule (dbt_project.yml sets quoting: false throughout).
## Sanitising to snake_case here is identifier hygiene, not transformation:
## no value is altered, only the column label. Applied ONLY on the landing
## path so no existing table's column names change.
sanitize_column_names <- function(nm) {
  out <- gsub("[^A-Za-z0-9_]+", "_", nm)
  out <- gsub("_+", "_", out)
  out <- sub("_$", "", out)
  tolower(out)
}

## Idempotency guard for append mode. Unlike "overwrite", a re-run of an
## append load silently doubles every row, so the guard is load-bearing, not
## a nicety: the VM's cron can re-run this loader on the same on-disk CSV
## (e.g. after a downstream failure) and NO_UPDATE alone does not protect the
## landing path, because a landing table that already exists is exactly the
## normal steady state rather than a reason to skip.
##
## A vintage is identified by (publication_date, source_url): gov.uk mints a
## new asset URL for each quarterly release, and publication_date alone would
## collide if two files were ever published the same day. Both columns come
## from the checker's own tagging.py (_publication_date / _source_url), so
## they are present on every row of every tidy CSV it writes.
##
## Returns TRUE when rows for this vintage are already in the landing table.
## A non-existent table is NOT already loaded (FALSE) - the first append
## creates it.
landing_vintage_loaded <- function(con, database, schema, table_name,
                                   publication_date, source_url) {
  if (!table_exists(con, database, schema, table_name)) {
    return(FALSE)
  }
  full_name <- paste(database, schema, toupper(table_name), sep = ".")
  n <- DBI::dbGetQuery(
    conn      = con,
    statement = paste0(
      "SELECT COUNT(*) AS N FROM ", full_name,
      " WHERE _PUBLICATION_DATE = ? AND _SOURCE_URL = ?"
    ),
    params    = list(publication_date, source_url)
  )
  as.numeric(n[[1, "N"]]) > 0
}

# Load each dataset ------------------------------------------------------------

## The connect+loop body below is wrapped in tryCatch(..., finally = ...)
## rather than a top-level on.exit(). on.exit() registered at top level
## (outside a function frame) persists for the rest of the interactive R
## session and re-fires — accumulating — on every subsequent source() of
## this script, since only function frames get a fresh on.exit scope on
## each call. tryCatch's finally is scoped to this single call and carries
## no such state across repeated interactive runs, while still guaranteeing
## the disconnect runs if readr::read_csv() or
## ascFuncs::snowflake_write_table() throws mid-loop (previously: a
## mid-loop error skipped the bare dbDisconnect() below entirely and the
## script terminated with the connection still open).
n_loaded <- 0L
n_skipped <- 0L
n_failed <- 0L

tryCatch(
  expr = {
    ## Raw DBI call, not a hand-rolled anti-pattern here — ascFuncs exports
    ## no session-info helper, and this is the exact pattern used
    ## identically in load_advicepro_demographics_to_snowflake.R,
    ## load_casework_locality_to_snowflake.R and
    ## load_member_orgs_to_snowflake.R (confirmed by grep across
    ## loaders/*.R). Matched to that convention here, including
    ## CURRENT_USER().
    session_info <- DBI::dbGetQuery(
      con,
      paste(
        "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(),",
        "CURRENT_USER()"
      )
    )
    log_info(
      "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
      "database={session_info[[1, 'CURRENT_DATABASE()']]} ",
      "user={session_info[[1, 'CURRENT_USER()']]}"
    )

    ## extract_drift_flag() and %||% now live in
    ## loaders/lib/extract_drift_flag.R so they can be unit-tested without a
    ## Snowflake connection (see tests/testthat/test-extract_drift_flag.R).
    source("lib/extract_drift_flag.R")

    ## Mirrors the existence check inside ascFuncs::snowflake_write_table -
    ## but that check is a local variable inside the function body, not
    ## exported or callable on its own (confirmed by reading ascFuncs
    ## source, 2026-07-14: ascFuncs has no exported table-exists/
    ## session-info helper at all). This loader needs the answer standalone,
    ## before calling snowflake_write_table, to decide first-time-load
    ## logging - so the duplication here is unavoidable given ascFuncs's
    ## current public API, not a shortcut taken in place of an available
    ## helper.
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

    # Load each dataset ------------------------------------------------------

    for (dataset in manifest$datasets) {
      id <- dataset$dataset_id
      status <- dataset$status

      if (status == "LIMITATION") {
        log_info("{id}: LIMITATION — not loaded (documented gap)")
        n_skipped <- n_skipped + 1L
        next
      }
      if (grepl("^FAILED_", status)) {
        log_warn(
          "{id}: {status} — {dataset$message %||% 'no message'} — NOT loaded"
        )
        n_failed <- n_failed + 1L
        next
      }
      ## Landing datasets accumulate vintages instead of being replaced -
      ## see the LANDING_DATASETS block at the top of this file.
      is_landing <- id %in% LANDING_DATASETS
      dataset_db <- if (is_landing) LANDING_DB else TARGET_DB
      dataset_schema <- if (is_landing) LANDING_SCHEMA else "PUBLIC"

      first_time_load <- FALSE
      if (status == "NO_UPDATE") {
        ## Landing datasets deliberately do NOT early-skip on table
        ## existence: the landing table existing is the normal steady state
        ## (it holds every prior vintage), and NO_UPDATE only says the SOURCE
        ## hasn't changed since the last EXTRACTION - not that this vintage
        ## has ever been loaded. The per-vintage guard below is the correct
        ## and sufficient stop for the append path.
        if (!is_landing && table_exists(con, dataset_db, dataset_schema, id)) {
          log_info("{id}: NO_UPDATE — table already current, skipping")
          n_skipped <- n_skipped + 1L
          next
        }
        if (is_landing) {
          log_info(
            "{id}: NO_UPDATE from source — landing path, deferring to the ",
            "per-vintage idempotency guard"
          )
        } else {
          log_info(
            "{id}: NO_UPDATE from source, but table doesn't exist in ",
            "Snowflake yet — first-time load"
          )
          first_time_load <- TRUE
        }
      } else if (status != "SUCCESS") {
        log_warn(
          "{id}: unrecognised status '{status}' — skipping defensively"
        )
        n_skipped <- n_skipped + 1L
        next
      }

      csv_path <- file.path(SOURCE_DIR, paste0(id, ".csv"))
      if (!file.exists(csv_path)) {
        log_warn(
          "{id}: {status} in manifest but {csv_path} missing — skipping"
        )
        n_failed <- n_failed + 1L
        next
      }

      df <- readr::read_csv(csv_path, show_col_types = FALSE)
      # NOTE: does NOT add a publication-date column here - source_
      # checker's own tagging.py already writes one (`_publication_date`,
      # same value, same source: resolved.inferred_publication_date /
      # last_known_good). Adding a second one collided case-insensitively
      # once Snowflake uppercases every column name ("duplicate column
      # name '_PUBLICATION_DATE'").
      df$`_RUN_ID` <- manifest$run_id
      df$`_RUN_AT` <- manifest$run_at
      df$`_DRIFT_FLAG` <- extract_drift_flag(dataset)

      drift_flag <- extract_drift_flag(dataset)
      if (!is.na(drift_flag)) {
        cli::cli_alert_warning(
          "{id}: drift flagged this run — {drift_flag} (see manifest ",
          "for detail)"
        )
      }

      if (is_landing) {
        ## --- Append / landing path (CLD quarterly series only) -------------
        ##
        ## operation = "append" is a real, supported mode of
        ## ascFuncs::snowflake_write_table() - confirmed by reading
        ## ascFuncs/R/snowflake.R (2026-09-16): the "append" branch
        ## existence-checks the table, then uses DBI::dbAppendTable() with a
        ## DBI::Id() identifier (no row clearing), falling back to a plain
        ## create when the table does not exist yet. It is NOT
        ## dbWriteTable(append = TRUE), which has a case-sensitivity
        ## landmine documented in that same file.
        pub_date <- unique(as.character(df[["_publication_date"]]))
        src_url <- unique(as.character(df[["_source_url"]]))
        if (length(pub_date) != 1 || is.na(pub_date) ||
              length(src_url) != 1 || is.na(src_url)) {
          ## Fail loudly rather than append rows that can never be
          ## de-duplicated or attributed to a vintage afterwards.
          log_warn(
            "{id}: landing path requires exactly one non-NA ",
            "_publication_date and _source_url across the file (got ",
            "{length(pub_date)} / {length(src_url)}) — NOT loaded"
          )
          n_failed <- n_failed + 1L
          next
        }

        ## snowflake_write_table() reroutes `database` to its _DEV
        ## counterpart when the session role is ROLE_DEV (admin#5,
        ## snowflake_route_dev_database() - read in ascFuncs/R/snowflake.R).
        ## The guard MUST probe the same database the write will land in,
        ## otherwise a ROLE_DEV run checks REFERENCE, finds nothing, and
        ## appends a duplicate vintage into REFERENCE_DEV on every run.
        guard_db <- ascFuncs::snowflake_route_dev_database(con, dataset_db)
        if (landing_vintage_loaded(
          con, guard_db, dataset_schema, id, pub_date, src_url
        )) {
          log_info(
            "{id}: vintage {pub_date} already present in ",
            "{guard_db}.{dataset_schema}.{toupper(id)} — skipping ",
            "(append is not idempotent on its own)"
          )
          n_skipped <- n_skipped + 1L
          next
        }

        names(df) <- sanitize_column_names(names(df))
        ascFuncs::snowflake_write_table(
          con        = con,
          table_name = id,
          data       = df,
          database   = dataset_db,
          schema     = dataset_schema,
          operation  = "append"
        )
        log_info(
          "{id}: appended vintage {pub_date}, {nrow(df)} rows -> ",
          "{guard_db}.{dataset_schema}.{toupper(id)}"
        )
        n_loaded <- n_loaded + 1L
        next
      }

      ascFuncs::snowflake_write_table(
        con        = con,
        table_name = id,
        data       = df,
        database   = TARGET_DB,
        schema     = "PUBLIC",
        operation  = "overwrite"
      )
      load_kind <- if (first_time_load) "first-time load" else "loaded"
      log_info(
        "{id}: {load_kind}, {nrow(df)} rows -> ",
        "{TARGET_DB}.PUBLIC.{toupper(id)}"
      )
      n_loaded <- n_loaded + 1L
    }
  },
  finally = DBI::dbDisconnect(con)
)

cli::cli_h2("Summary")
cli::cli_alert_success(
  "{n_loaded} loaded, {n_skipped} skipped, {n_failed} failed"
)

if (n_failed > 0) {
  stop(
    n_failed, " dataset(s) failed to load — see warnings above.",
    call. = FALSE
  )
}
