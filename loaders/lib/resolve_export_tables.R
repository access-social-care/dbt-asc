## Explicit allowlist of tables intended for export by
## export_la_queries_to_s3.R, plus the pure resolution logic that decides
## which discovered tables actually get exported.
##
## Pulled out of the loader script so it can be sourced and tested without
## connecting to Snowflake (tests/testthat/test-resolve_export_tables.R).
##
## Background (access-social-care/admin#5): the loader used to discover and
## export *every* table in ANALYTICS.PUBLIC_LA_PRODUCT with no name filter.
## That is why the unsuppressed mart_la_query_summary table was pushed to
## S3/Redis alongside the small-number-suppressed mart_glos_la_* marts - a
## real data exposure, already remediated by purging it from S3/Redis and by
## relocating mart_la_query_summary out of this schema entirely (PR #74).
## The schema-wide discovery pattern itself was the root cause and would
## silently repeat the incident the next time any non-suppressed model
## landed in this schema. This allowlist is the fix: only tables named here
## are ever exported, regardless of what else exists in the schema.
##
## Keep this list in sync with models/marts/la_product/schema.yml - every
## mart_glos_la_* model documented there should appear here in Snowflake's
## unquoted (UPPERCASE) form.

ALLOWED_LA_EXPORT_TABLES <- c(
  "MART_GLOS_LA_ACTIVITY_SUMMARY_1M",
  "MART_GLOS_LA_ACTIVITY_SUMMARY_3M",
  "MART_GLOS_LA_ACTIVITY_SUMMARY_6M",
  "MART_GLOS_LA_ACTIVITY_SUMMARY_9M",
  "MART_GLOS_LA_ACTIVITY_SUMMARY_12M",
  "MART_GLOS_LA_DEMOGRAPHICS_1M",
  "MART_GLOS_LA_DEMOGRAPHICS_3M",
  "MART_GLOS_LA_DEMOGRAPHICS_6M",
  "MART_GLOS_LA_DEMOGRAPHICS_9M",
  "MART_GLOS_LA_DEMOGRAPHICS_12M",
  "MART_GLOS_LA_LEGAL_LETTERS_1M",
  "MART_GLOS_LA_LEGAL_LETTERS_3M",
  "MART_GLOS_LA_LEGAL_LETTERS_6M",
  "MART_GLOS_LA_LEGAL_LETTERS_9M",
  "MART_GLOS_LA_LEGAL_LETTERS_12M",
  "MART_GLOS_LA_LOCALITY_OVERVIEW_1M",
  "MART_GLOS_LA_LOCALITY_OVERVIEW_3M",
  "MART_GLOS_LA_LOCALITY_OVERVIEW_6M",
  "MART_GLOS_LA_LOCALITY_OVERVIEW_9M",
  "MART_GLOS_LA_LOCALITY_OVERVIEW_12M",
  "MART_GLOS_LA_QUERY_SEGMENTS_1M",
  "MART_GLOS_LA_QUERY_SEGMENTS_3M",
  "MART_GLOS_LA_QUERY_SEGMENTS_6M",
  "MART_GLOS_LA_QUERY_SEGMENTS_9M",
  "MART_GLOS_LA_QUERY_SEGMENTS_12M",
  "MART_GLOS_LA_QUERIES_OVER_TIME_1M",
  "MART_GLOS_LA_QUERIES_OVER_TIME_3M",
  "MART_GLOS_LA_QUERIES_OVER_TIME_6M",
  "MART_GLOS_LA_QUERIES_OVER_TIME_9M",
  "MART_GLOS_LA_QUERIES_OVER_TIME_12M",
  "MART_GLOS_LA_QUERY_SOURCE_1M",
  "MART_GLOS_LA_QUERY_SOURCE_3M",
  "MART_GLOS_LA_QUERY_SOURCE_6M",
  "MART_GLOS_LA_QUERY_SOURCE_9M",
  "MART_GLOS_LA_QUERY_SOURCE_12M"
)

## Decides which tables to export given what's actually in the schema.
##
## Never returns a table that isn't in `allowed`, no matter what's in
## `discovered` - that's the whole point. Tables in `allowed` but missing
## from `discovered`, and tables in `discovered` but not in `allowed`, are
## both surfaced separately so the caller can log them rather than silently
## dropping or silently exporting them.
##
## @param discovered Character vector of table names found in the schema
##   (e.g. via INFORMATION_SCHEMA.TABLES).
## @param allowed Character vector of table names permitted to export.
##   Defaults to ALLOWED_LA_EXPORT_TABLES.
##
## @return A list with:
##   - export: allowed tables that are present, in `allowed`'s order
##   - missing: allowed tables not found in the schema (stale allowlist, or
##     dbt hasn't run yet)
##   - unexpected: tables present in the schema but not in the allowlist -
##     these are NEVER exported, but are worth knowing about (this is the
##     signal that would have caught the original incident)
resolve_export_tables <- function(
  discovered,
  allowed = ALLOWED_LA_EXPORT_TABLES
) {
  export    <- allowed[allowed %in% discovered]
  missing   <- allowed[!(allowed %in% discovered)]
  unexpected <- discovered[!(discovered %in% allowed)]

  list(export = export, missing = missing, unexpected = unexpected)
}
