## Load AdvicePro persons-with-care-needs demographics to Snowflake
##
## Source: AdvicePro ReportRunner report HS6T5CH9
## Target: CASEWORK.PUBLIC.ADVICEPRO_DEMOGRAPHICS_PERSONS
##
## Why a second demographics table: report FD7DXGL4 (ADVICEPRO_DEMOGRAPHICS)
## only covers cases that have an equalities form (about a quarter of cases) and
## has no persons count. HS6T5CH9 is roughly one row per case and carries
## "Number of Person with Care Needs", which the ILTA return counts (people, not
## cases). It has NO case reference, so it cannot be joined to ADVICEPRO_CASEWORK
## and carries no local authority or member. Used by stg_ilta_advicepro only.
##
## Strategy: full replace on every run (the report is a rolling extract).
##
## Usage:
##   Rscript loaders/load_advicepro_persons_to_snowflake.R
##   (run from dbt-asc/ root, or via absolute path from cron)

library(ascFuncs)
library(tidyverse)
library(logger)
library(cli)

# Config ------------------------------------------------------------------

REPORT_KEY   <- "HS6T5CH9"
TARGET_TABLE <- "ADVICEPRO_DEMOGRAPHICS_PERSONS"
TARGET_DB    <- "CASEWORK"

# Pull from AdvicePro -----------------------------------------------------

cli::cli_h1("Loading AdvicePro persons demographics from ReportRunner")
cli::cli_alert_info("Report: {REPORT_KEY} -> {TARGET_DB}.PUBLIC.{TARGET_TABLE}")

df_raw <- tryCatch(
  ascFuncs::query_advicepro_report(REPORT_KEY),
  error = function(e) {
    log_error("API query failed: {e$message}")
    stop("Persons demographics report fetch failed - see above.", call. = FALSE)
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
    "Data may be truncated - check report pagination or split by year."
  )
}

# Normalise ---------------------------------------------------------------

df <- df_raw %>%
  set_names(names(.) %>% gsub(" ", "_", .) %>% tolower()) %>%
  ## Same placeholder handling as load_advicepro_demographics_to_snowflake.R
  mutate(
    across(where(is.character), ~na_if(.x, "")),
    across(where(is.character), ~na_if(.x, "[Not Specified]"))
  )

## The persons column is spelled "Number of Person with Care Needss" in AdvicePro
## (sic). Fail loudly if the report layout changes rather than load nonsense.
expected <- c(
  "age_range", "ethnic_origin", "gender", "do_you_have_a_disability",
  "what_type_of_disability_do_you_consider_you_have", "case_open_date",
  "number_of_person_with_care_needss"
)
missing_cols <- setdiff(expected, names(df))
if (length(missing_cols) > 0) {
  stop(
    "Report layout changed. Missing columns: ", paste(missing_cols, collapse = ", "),
    ". Actual: ", paste(names(df), collapse = ", "),
    call. = FALSE
  )
}

df <- df %>%
  transmute(
    record_id = row_number(),
    case_open_date = as.Date(case_open_date, format = "%d/%m/%Y"),
    persons = as.integer(number_of_person_with_care_needss),
    age_range,
    gender,
    ethnic_origin,
    do_you_have_a_disability,
    what_type_of_disability_do_you_consider_you_have
  )

if (anyNA(df$case_open_date)) {
  log_warn("{sum(is.na(df$case_open_date))} rows have an unparseable Case Open Date")
}
if (anyNA(df$persons) || any(df$persons < 1, na.rm = TRUE)) {
  stop("Persons count has NA or values below 1 - check the report.", call. = FALSE)
}

log_info("Final: {nrow(df)} rows, {sum(df$persons)} persons ready for upload")

# Upload to Snowflake -----------------------------------------------------

cli::cli_h2("Uploading to Snowflake")
con <- ascFuncs::connect_snowflake(database = TARGET_DB, role = NULL)
on.exit(DBI::dbDisconnect(con), add = TRUE)

session_info <- DBI::dbGetQuery(
  con,
  "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
)
log_info(
  "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
  "database={session_info[[1, 'CURRENT_DATABASE()']]} ",
  "schema={session_info[[1, 'CURRENT_SCHEMA()']]}"
)

ascFuncs::snowflake_write_table(
  con        = con,
  table_name = TARGET_TABLE,
  data       = df,
  database   = TARGET_DB,
  schema     = "PUBLIC",
  operation  = "overwrite"
)

log_info("Upload complete: {nrow(df)} rows -> {TARGET_DB}.PUBLIC.{TARGET_TABLE}")
cli::cli_alert_success("{TARGET_TABLE} loaded: {nrow(df)} rows")
