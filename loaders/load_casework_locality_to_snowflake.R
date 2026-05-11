## Load casework locality data to Snowflake via findthatpostcode.uk API
##
## Source: AdvicePro ReportRunner report PWVDK69X (case_reference + client_postcode)
## Target: CASEWORK.PUBLIC.CASEWORK_LOCALITY
##
## Incremental logic:
##   1. Pull all case_reference + client_postcode from AdvicePro report
##   2. Compare against existing case_references in Snowflake
##   3. Only call the API for new case_references (skips already-resolved ones)
##   4. Append new rows — existing rows are never touched
##
## First run (table doesn't exist yet): creates the table.
## Subsequent runs: appends only new rows.
##
## Calls findthatpostcode.uk one postcode at a time with a 5s sleep between
## calls to be polite to the free API. Deduplicates postcodes within each
## batch so cases sharing a postcode only trigger one API call.
##
## Runs daily via cron. Safe to re-run.
##
## Usage:
##   Rscript loaders/load_casework_locality_to_snowflake.R
##   (run from dbt-asc/ root, or via absolute path from cron)

library(ascFuncs)
library(tidyverse)
library(httr)
library(logger)
library(cli)

# Config ------------------------------------------------------------------

REPORT_KEY   <- "PWVDK69X"
TARGET_TABLE <- "CASEWORK_LOCALITY"
TARGET_DB    <- "CASEWORK"

# Helpers -----------------------------------------------------------------

## Call findthatpostcode.uk for a single postcode and return a one-row tibble.
## Returns NA columns on lookup failure so case_references are never dropped.
postcode_json_getter <- function(postcode) {
  Sys.sleep(5)
  url  <- paste0("https://findthatpostcode.uk/postcodes/", gsub(" ", "%20", postcode), ".json")
  site <- httr::GET(url) %>% httr::content()
  a    <- site$data$attributes

  if (is.null(a)) {
    log_warn("No result for postcode: {postcode}")
    return(tibble(
      postcode                              = postcode,
      la_name                               = NA_character_,
      local_authority_code                  = NA_character_,
      ward                                  = NA_character_,
      ward_code                             = NA_character_,
      constituency_name                     = NA_character_,
      constituency_code                     = NA_character_,
      mso_area_name                         = NA_character_,
      mso_area_code                         = NA_character_,
      lso_area_name                         = NA_character_,
      lso_area_code                         = NA_character_,
      nhs_icb_name                          = NA_character_,
      nhs_icb_code                          = NA_character_,
      urban_rural_classification_code       = NA_character_,
      urban_rural_classification_name       = NA_character_,
      urban_conurbation_classification_code = NA_character_,
      urban_conurbation_classification_name = NA_character_
    ))
  }

  tibble(
    postcode                              = postcode,
    la_name                               = a$laua_name            %||% NA_character_,
    local_authority_code                  = a$laua                 %||% NA_character_,
    ward                                  = a$ward_name            %||% NA_character_,
    ward_code                             = a$ward                 %||% NA_character_,
    constituency_name                     = a$pcon_name            %||% NA_character_,
    constituency_code                     = a$pcon                 %||% NA_character_,
    mso_area_name                         = a$msoa21_name          %||% NA_character_,
    mso_area_code                         = a$msoa21               %||% NA_character_,
    lso_area_name                         = a$lsoa21_name          %||% NA_character_,
    lso_area_code                         = a$lsoa21               %||% NA_character_,
    nhs_icb_name                          = a$icb_name             %||% NA_character_,
    nhs_icb_code                          = a$icb                  %||% NA_character_,
    urban_rural_classification_code       = a$ru11ind$code         %||% NA_character_,
    urban_rural_classification_name       = a$ru11ind$description  %||% NA_character_,
    urban_conurbation_classification_code = a$bua11                %||% NA_character_,
    urban_conurbation_classification_name = a$bua11_name           %||% NA_character_
  )
}

# Connect to Snowflake ----------------------------------------------------

cli::cli_h1("Casework Locality — incremental load")
cli::cli_alert_info("Target: {TARGET_DB}.PUBLIC.{TARGET_TABLE}")

con <- ascFuncs::connect_snowflake(database = TARGET_DB)
on.exit(DBI::dbDisconnect(con), add = TRUE)

session_info <- DBI::dbGetQuery(
  con,
  "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER()"
)
log_info(
  "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
  "database={session_info[[1, 'CURRENT_DATABASE()']]} ",
  "user={session_info[[1, 'CURRENT_USER()']]}"
)

# Pull AdvicePro report ---------------------------------------------------

log_info("Querying AdvicePro report {REPORT_KEY}")
ap_raw <- ascFuncs::query_advicepro_report(REPORT_KEY)
log_info("AdvicePro returned {nrow(ap_raw)} rows")

ap <- ap_raw %>%
  dplyr::select(case_reference, postcode = client_postcode) %>%
  dplyr::filter(!is.na(postcode), trimws(postcode) != "")

log_info("{nrow(ap)} rows with a postcode ({n_distinct(ap$case_reference)} unique case_references)")

if (nrow(ap) == 0) {
  log_warn("No rows with postcodes — nothing to do")
  quit(save = "no", status = 0)
}

# Check what's already in Snowflake ---------------------------------------

target_full <- paste(TARGET_DB, "PUBLIC", TARGET_TABLE, sep = ".")

table_exists <- tryCatch({
  DBI::dbGetQuery(con, paste0("SELECT 1 FROM ", target_full, " LIMIT 0"))
  TRUE
}, error = function(e) FALSE)

if (table_exists) {
  existing_ids <- DBI::dbGetQuery(
    con,
    paste0('SELECT DISTINCT "case_reference" FROM ', target_full)
  ) %>% dplyr::pull(case_reference)
  log_info("{length(existing_ids)} case_references already in Snowflake")
} else {
  existing_ids <- character(0)
  log_info("Table does not exist yet — full load")
}

new_cases <- ap %>% dplyr::filter(!case_reference %in% existing_ids)
log_info("{nrow(new_cases)} new case_references to process")

if (nrow(new_cases) == 0) {
  log_info("Nothing new — exiting")
  quit(save = "no", status = 0)
}

# Call findthatpostcode.uk (deduplicated) ---------------------------------

unique_pcs <- unique(new_cases$postcode)
log_info(
  "Calling findthatpostcode.uk — {length(unique_pcs)} unique postcodes ",
  "across {nrow(new_cases)} new cases (5s sleep between calls)"
)

postcode_results <- unique_pcs %>%
  set_names() %>%
  map(safely(postcode_json_getter))

errors <- postcode_results %>% map("error") %>% compact()
if (length(errors) > 0) {
  log_warn("{length(errors)} postcode(s) errored: {paste(names(errors), collapse = ', ')}")
}

postcode_lookup <- postcode_results %>%
  map("result") %>%
  compact() %>%
  bind_rows()

log_info(
  "API results: {sum(!is.na(postcode_lookup$la_name))} resolved, ",
  "{sum(is.na(postcode_lookup$la_name))} not found"
)

# Join back to new case_references ----------------------------------------

new_locality <- new_cases %>%
  dplyr::left_join(postcode_lookup, by = "postcode") %>%
  dplyr::select(-postcode)

log_info("{nrow(new_locality)} rows ready to append")

# Write to Snowflake ------------------------------------------------------

cli::cli_h2("Writing to Snowflake")
table_id <- DBI::Id(database = TARGET_DB, schema = "PUBLIC", table = TARGET_TABLE)

if (table_exists) {
  DBI::dbAppendTable(con, table_id, new_locality)
  log_info("Appended {nrow(new_locality)} rows to {target_full}")
} else {
  DBI::dbWriteTable(con, table_id, new_locality)
  ascFuncs::snowflake_grant_select(con, TARGET_TABLE, schema = "PUBLIC", database = TARGET_DB)
  log_info("Created {target_full} with {nrow(new_locality)} rows")
}

log_info("Done")
