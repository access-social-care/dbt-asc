## Tests for resolve_export_tables() (loaders/lib/resolve_export_tables.R).
##
## Pure function, no Snowflake connection needed - just character vectors of
## table names as they'd come back from INFORMATION_SCHEMA.TABLES.
##
## Run: Rscript -e "testthat::test_file('tests/testthat/test-resolve_export_
##   tables.R')" from the dbt-asc/ root.
##
## testthat::test_file() runs with the test file's own directory as the
## working directory, so the source() path is relative to tests/testthat/,
## not the repo root.

source("../../loaders/lib/resolve_export_tables.R")

testthat::test_that("only allowlisted tables are exported", {
  discovered <- c("MART_GLOS_LA_ACTIVITY_SUMMARY_1M", "SOME_OTHER_TABLE")
  result <- resolve_export_tables(
    discovered,
    allowed = c("MART_GLOS_LA_ACTIVITY_SUMMARY_1M")
  )
  testthat::expect_identical(
    result$export,
    "MART_GLOS_LA_ACTIVITY_SUMMARY_1M"
  )
})

testthat::test_that("unsuppressed table outside allowlist is never exported", {
  ## Regression test for admin#5: mart_la_query_summary existed alongside
  ## the suppressed mart_glos_la_* marts in PUBLIC_LA_PRODUCT and was
  ## exported by the old schema-wide discovery logic. This is exactly that
  ## scenario - the un-allowlisted table must never appear in export.
  discovered <- c(
    "MART_GLOS_LA_ACTIVITY_SUMMARY_1M",
    "MART_LA_QUERY_SUMMARY"
  )
  result <- resolve_export_tables(
    discovered,
    allowed = c("MART_GLOS_LA_ACTIVITY_SUMMARY_1M")
  )
  testthat::expect_false("MART_LA_QUERY_SUMMARY" %in% result$export)
  testthat::expect_identical(result$unexpected, "MART_LA_QUERY_SUMMARY")
})

testthat::test_that("export preserves allowlist order, not discovery order", {
  discovered <- c("TABLE_B", "TABLE_A")
  result <- resolve_export_tables(
    discovered,
    allowed = c("TABLE_A", "TABLE_B")
  )
  testthat::expect_identical(result$export, c("TABLE_A", "TABLE_B"))
})

testthat::test_that("allowlisted table missing from schema is flagged", {
  discovered <- c("TABLE_A")
  result <- resolve_export_tables(
    discovered,
    allowed = c("TABLE_A", "TABLE_B")
  )
  testthat::expect_identical(result$export, "TABLE_A")
  testthat::expect_identical(result$missing, "TABLE_B")
})

testthat::test_that("empty discovery yields empty export and full missing", {
  result <- resolve_export_tables(
    character(0),
    allowed = c("TABLE_A", "TABLE_B")
  )
  testthat::expect_length(result$export, 0)
  testthat::expect_identical(result$missing, c("TABLE_A", "TABLE_B"))
  testthat::expect_length(result$unexpected, 0)
})

testthat::test_that("default allowlist covers all 7 la_product mart families", {
  ## Matches models/marts/la_product/schema.yml - 7 families x 5 time
  ## windows (1m/3m/6m/9m/12m) = 35 tables.
  testthat::expect_length(ALLOWED_LA_EXPORT_TABLES, 35)
  testthat::expect_true(all(grepl("^MART_GLOS_LA_", ALLOWED_LA_EXPORT_TABLES)))
  testthat::expect_identical(
    ALLOWED_LA_EXPORT_TABLES,
    unique(ALLOWED_LA_EXPORT_TABLES)
  )
})
