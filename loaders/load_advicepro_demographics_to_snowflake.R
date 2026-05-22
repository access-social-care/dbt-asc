## Load AdvicePro client demographics to Snowflake
##
## Source: AdvicePro ReportRunner report FD7DXGL4
## Target: CASEWORK.PUBLIC.ADVICEPRO_DEMOGRAPHICS
##
## Strategy: Full replace on every run.
##   Demographics are recorded at time of case creation and do not change.
##   Full replace is safe and keeps the table in sync with the AdvicePro source.
##
## The table is joined to ADVICEPRO_CASEWORK on case_reference by stg_advicepro
## to add age/agegroup dimensions to the LA Data Product.
##
## Usage:
##   Rscript loaders/load_advicepro_demographics_to_snowflake.R
##   (run from dbt-asc/ root, or via absolute path from cron)

library(ascFuncs)
library(tidyverse)
library(logger)
library(cli)

# Config ------------------------------------------------------------------

REPORT_KEY   <- "FD7DXGL4"
TARGET_TABLE <- "ADVICEPRO_DEMOGRAPHICS"
TARGET_DB    <- "CASEWORK"

# Pull from AdvicePro -----------------------------------------------------

cli::cli_h1("Loading AdvicePro Demographics from ReportRunner")
cli::cli_alert_info("Report: {REPORT_KEY} -> {TARGET_DB}.PUBLIC.{TARGET_TABLE}")

log_info("Querying AdvicePro report {REPORT_KEY}")
df_raw <- tryCatch(
  ascFuncs::query_advicepro_report(REPORT_KEY),
  error = function(e) {
    log_error("API query failed: {e$message}")
    stop("Demographics report fetch failed — see above.", call. = FALSE)
  }
)

if (is.null(df_raw) || nrow(df_raw) == 0) {
  stop("Report returned no rows. Check report ID and API credentials.", call. = FALSE)
}

log_info("Fetched {nrow(df_raw)} rows, {ncol(df_raw)} columns")
log_info("Columns: {paste(names(df_raw), collapse = ', ')}")

if (nrow(df_raw) >= 1000) {
  log_warn(
    "Row count {nrow(df_raw)} is at/above the 1000-row API limit. ",
    "Data may be truncated — check report pagination or split by year."
  )
}

# Normalise ---------------------------------------------------------------

df <- df_raw %>%
  ## Normalise column names: lowercase + underscores (same convention as other loaders)
  set_names(names(.) %>% gsub(" ", "_", .) %>% tolower()) %>%
  ## Replace AdvicePro placeholders with NA
  mutate(
    across(where(is.character), ~na_if(.x, "")),
    across(where(is.character), ~na_if(.x, "[Not Specified]"))
  )

log_info("Normalised column names: {paste(names(df), collapse = ', ')}")

## Verify the join key is present
if (!"case_reference" %in% names(df)) {
  stop(
    "Expected column 'case_reference' not found after normalisation. ",
    "Actual columns: ", paste(names(df), collapse = ", "),
    call. = FALSE
  )
}

## Normalise case_reference before dedup — trim whitespace and uppercase
## to catch duplicates that differ only by padding or case (common in AdvicePro exports)
df <- df %>%
  mutate(case_reference = toupper(trimws(case_reference)))

## Dedup on case_reference — keep the first occurrence if duplicates exist
n_before <- nrow(df)
df <- df %>% distinct(case_reference, .keep_all = TRUE)
n_dupes <- n_before - nrow(df)
if (n_dupes > 0) {
  log_warn("Dropped {n_dupes} duplicate case_reference rows")
}

log_info("Final: {nrow(df)} rows ready for upload")

# Upload to Snowflake -----------------------------------------------------

cli::cli_h2("Uploading to Snowflake")
log_info("Connecting to {TARGET_DB}")
con <- ascFuncs::connect_snowflake(database = TARGET_DB, role = NULL)

session_info <- DBI::dbGetQuery(
  con,
  "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER()"
)
log_info(
  "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
  "database={session_info[[1, 'CURRENT_DATABASE()']]} ",
  "schema={session_info[[1, 'CURRENT_SCHEMA()']]}"
)

## Full replace — demographics are immutable once written
ascFuncs::snowflake_write_table(
  con        = con,
  table_name = TARGET_TABLE,
  data       = df,
  database   = TARGET_DB,
  schema     = "PUBLIC",
  overwrite  = TRUE
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

log_info("Upload complete: {nrow(df)} rows -> {TARGET_DB}.PUBLIC.{TARGET_TABLE}")
cli::cli_alert_success(
  "ADVICEPRO_DEMOGRAPHICS loaded: {nrow(df)} rows"
)

