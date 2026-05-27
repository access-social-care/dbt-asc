# snowflake_staleness_check.R
#
# Purpose: Cross-system Snowflake health check — runs after dbt build as a
#          non-fatal observability step. Checks INFORMATION_SCHEMA.LAST_ALTERED
#          for all operational source databases.
#
# Why INFORMATION_SCHEMA.LAST_ALTERED (not dbt source freshness)?
#   - source freshness uses data-level timestamps (opened_on, created_at) which
#     reflect data age, not whether the ETL actually ran.
#   - LAST_ALTERED is updated on TRUNCATE+INSERT — it directly reflects ETL
#     execution. A table untouched for >30d means the ETL has not run.
#   - ANALYTICS tables are NOT checked here — dbt build just ran, so they are
#     always fresh at this point. dbt run_results.json is the authoritative
#     record for model-level freshness.
#
# Databases checked: CASEWORK, AVA, HELPLINES
# REFERENCE is skipped — static lookup tables with no ETL owner (see admin#9).
# ANALYTICS is skipped — freshness guaranteed by the preceding dbt build.
#
# Exit codes:
#   0 — all tables fresh or within threshold
#   0 — stale tables found (exits 0 so pipeline does not fail — this is
#       observability only; real alerts go via GitHub issue / manual review)
#
# Called from run_pipeline.sh Stage 4.

suppressPackageStartupMessages({
  library(ascFuncs)
  library(dplyr)
  library(logger)
})

STALE_DAYS  <- 30
DATABASES   <- c("CASEWORK", "AVA", "HELPLINES")

log_info("=== Snowflake staleness check at {Sys.time()} ===")
log_info("Threshold: tables not updated in >{STALE_DAYS} days flagged as stale")

# -----------------------------------------------------------------------
# Query each database
# -----------------------------------------------------------------------
check_database <- function(db) {
  con <- tryCatch(
    connect_snowflake(database = db),
    error = function(e) {
      log_warn("  Could not connect to {db}: {e$message}")
      NULL
    }
  )
  if (is.null(con)) return(NULL)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  result <- tryCatch(
    DBI::dbGetQuery(con, "
      SELECT
        TABLE_NAME,
        ROW_COUNT,
        LAST_ALTERED,
        DATEDIFF('day', LAST_ALTERED, CURRENT_TIMESTAMP) AS days_since_update
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'PUBLIC'
        AND TABLE_TYPE   = 'BASE TABLE'
      ORDER BY days_since_update DESC
    "),
    error = function(e) {
      log_warn("  INFORMATION_SCHEMA query failed for {db}: {e$message}")
      NULL
    }
  )
  if (!is.null(result)) result$DATABASE <- db
  result
}

all_tables <- bind_rows(lapply(DATABASES, check_database))

if (nrow(all_tables) == 0) {
  log_warn("No tables returned from any database — check Snowflake connectivity")
  quit(save = "no", status = 0)
}

# -----------------------------------------------------------------------
# Report
# -----------------------------------------------------------------------
stale <- all_tables %>%
  filter(days_since_update > STALE_DAYS) %>%
  arrange(desc(days_since_update))

fresh <- all_tables %>%
  filter(days_since_update <= STALE_DAYS)

log_info("--- Fresh tables ({nrow(fresh)}) ---")
for (i in seq_len(nrow(fresh))) {
  r <- fresh[i, ]
  log_info("  OK  {r$DATABASE}.{r$TABLE_NAME}: {r$ROW_COUNT} rows, updated {r$days_since_update}d ago")
}

if (nrow(stale) > 0) {
  log_warn("--- STALE tables ({nrow(stale)}) ---")
  for (i in seq_len(nrow(stale))) {
    r <- stale[i, ]
    log_warn("  STALE  {r$DATABASE}.{r$TABLE_NAME}: {r$ROW_COUNT} rows, last updated {r$days_since_update}d ago ({r$LAST_ALTERED})")
  }
  log_warn("Action: check the ETL pipelines for the stale databases above")
} else {
  log_info("All tables fresh (all updated within {STALE_DAYS} days)")
}

log_info("=== Staleness check complete ===")
quit(save = "no", status = 0)
