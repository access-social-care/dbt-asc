## Load official member organisation names from Monday.com to Snowflake.
##
## Source: Monday.com contracts board (6737029126)
##         Group: "Organisations with signed contract/agreement"
## Target: REFERENCE.PUBLIC.MEMBER_ORGANISATIONS
##
## Runs daily via cron. It is safe to re-run — fully replaces the table each time.
## Monday.com is the source of truth for official organisation names.
##
## Usage:
##   Rscript loaders/load_member_orgs_to_snowflake.R
##   (run from dbt-asc/ root, or via absolute path from cron)
##
## Output table columns:
##   monday_name     — Official organisation name (as displayed on Monday.com)
##   service_type    — Standardised contract type (see case_when below)
##   contract_group  — Raw group title from the contracts board

library(ascFuncs)
library(tidyverse)
library(logger)
library(cli)

# Config ------------------------------------------------------------------

CONTRACTS_BOARD_ID <- "6737029126"
CONTRACTS_GROUP    <- "Organisations with signed contract/agreement"
TARGET_DB          <- "REFERENCE"
TARGET_TABLE       <- "MEMBER_ORGANISATIONS"

# Pull from Monday.com ----------------------------------------------------

cli::cli_h1("Loading Member Organisations from Monday.com")
cli::cli_alert_info("Board: {CONTRACTS_BOARD_ID} → {TARGET_DB}.PUBLIC.{TARGET_TABLE}")

log_info("Querying contracts board {CONTRACTS_BOARD_ID}")
board <- ascFuncs::query_monday_board(
  CONTRACTS_BOARD_ID,
  columns_keep = c("Service")   ## only the service status column is needed
)

raw_df <- board$df
log_info("Board returned {nrow(raw_df)} total rows across {length(unique(raw_df$group_title))} group(s)")

# Filter and clean --------------------------------------------------------

orgs_df <- raw_df %>%
  dplyr::filter(group_title == CONTRACTS_GROUP) %>%
  dplyr::select(
    monday_name    = item_name,
    service_type   = Service,
    contract_group = group_title
  ) %>%
  ## Standardise service types — keep in sync with advicePro_queries/validate_board_membership.R
  dplyr::mutate(
    service_type = dplyr::case_when(
      grepl("Casework Membership 1 day per week",     service_type) ~ "Casework & Advice",
      grepl("Advice & Casework \\(bespoke\\)",        service_type) ~ "Casework & Advice",
      grepl("Advice & Casework Membership 4days pm",  service_type) ~ "Casework & Advice",
      grepl("Advice & Casework 2days pm",             service_type) ~ "Casework & Advice",
      grepl("Advice & Casework 1day pm",              service_type) ~ "Casework & Advice",
      grepl("Advice Membership",                      service_type) ~ "Advice Membership",
      TRUE ~ service_type
    )
  )

n_orgs <- nrow(orgs_df)
n_excl <- nrow(raw_df) - nrow(raw_df %>% dplyr::filter(group_title == CONTRACTS_GROUP))

cli::cli_alert_info(
  "{n_orgs} org(s) in '{CONTRACTS_GROUP}' ({n_excl} row(s) from other groups excluded)"
)

if (n_orgs == 0) {
  stop(
    "No organisations found in group '", CONTRACTS_GROUP, "'. ",
    "Check that the group name hasn't been renamed on the board.",
    call. = FALSE
  )
}

# Upload to Snowflake -----------------------------------------------------

cli::cli_h2("Uploading to Snowflake")
log_info("Connecting to {TARGET_DB}")
con <- ascFuncs::connect_snowflake(database = TARGET_DB)
on.exit(DBI::dbDisconnect(con), add = TRUE)

session_info <- DBI::dbGetQuery(
  con,
  "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER()"
)
log_info(
  "Snowflake session: role={session_info[[1, 'CURRENT_ROLE()']]} ",
  "database={session_info[[1, 'CURRENT_DATABASE()']]} ",
  "schema={session_info[[1, 'CURRENT_SCHEMA()']]}"
)

## Full replace on every run — Monday.com is the source of truth.
## snowflake_write_table() uses TRUNCATE+INSERT when the table already exists,
## which avoids needing DROP permission for the cron role.

ascFuncs::snowflake_write_table(
  con        = con,
  table_name = TARGET_TABLE,
  data       = orgs_df,
  database   = TARGET_DB,
  schema     = "PUBLIC",
  overwrite  = TRUE
)

## dbt reads REFERENCE directly, so grant it here as well. This keeps the
## table usable even before future grants are normalised in admin SQL.
tryCatch(
  {
    ascFuncs::snowflake_grant_select(
      con,
      TARGET_TABLE,
      schema = "PUBLIC",
      database = TARGET_DB,
      role = "ROLE_DBT_TRANSFORM"
    )
  },
  error = function(e) {
    log_warn("Could not grant SELECT on {TARGET_DB}.PUBLIC.{TARGET_TABLE} to ROLE_DBT_TRANSFORM: {conditionMessage(e)}")
  }
)

row_count <- DBI::dbGetQuery(
  con,
  paste0("SELECT COUNT(*) AS n_rows FROM ", TARGET_DB, ".PUBLIC.", TARGET_TABLE)
)$N_ROWS[[1]]

cli::cli_alert_success(
  "Loaded {TARGET_DB}.PUBLIC.{TARGET_TABLE} — {row_count} organisation(s)"
)

## Log breakdown by service type
orgs_df %>%
  dplyr::count(service_type, name = "n") %>%
  dplyr::arrange(dplyr::desc(n)) %>%
  print()

log_info("Done.")
