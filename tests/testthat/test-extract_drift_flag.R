## Tests for extract_drift_flag() (loaders/lib/extract_drift_flag.R).
##
## Pure function, no Snowflake connection or manifest.json needed - just
## dataset$provenance$warnings shapes as they'd appear after
## jsonlite::fromJSON(..., simplifyDataFrame = FALSE).
##
## Run: Rscript -e "testthat::test_file('tests/testthat/test-extract_drift_
##   flag.R')" from the dbt-asc/ root.
##
## testthat::test_file() runs with the test file's own directory as the
## working directory, so the source() path is relative to tests/testthat/,
## not the repo root.

source("../../loaders/lib/extract_drift_flag.R")

testthat::test_that("no warnings returns NA", {
  dataset <- list(provenance = list(warnings = NULL))
  testthat::expect_true(is.na(extract_drift_flag(dataset)))

  dataset_empty <- list(provenance = list(warnings = list()))
  testthat::expect_true(is.na(extract_drift_flag(dataset_empty)))
})

testthat::test_that("single SCHEMA_DRIFT warning returns 'SCHEMA_DRIFT'", {
  dataset <- list(provenance = list(warnings = list(
    list(code = "SCHEMA_DRIFT", message = "column added", details = NULL)
  )))
  testthat::expect_identical(extract_drift_flag(dataset), "SCHEMA_DRIFT")
})

testthat::test_that("multiple distinct drift codes combine with '+'", {
  dataset <- list(provenance = list(warnings = list(
    list(code = "SCHEMA_DRIFT", message = "x", details = NULL),
    list(code = "SEMANTIC_DRIFT_SUSPECTED", message = "y", details = NULL)
  )))
  testthat::expect_identical(
    extract_drift_flag(dataset),
    "SCHEMA_DRIFT+SEMANTIC_DRIFT_SUSPECTED"
  )
})

testthat::test_that("duplicate codes are deduped", {
  dataset <- list(provenance = list(warnings = list(
    list(code = "SCHEMA_DRIFT", message = "x", details = NULL),
    list(code = "SCHEMA_DRIFT", message = "x again", details = NULL)
  )))
  testthat::expect_identical(extract_drift_flag(dataset), "SCHEMA_DRIFT")
})

testthat::test_that("non-drift warning codes are ignored", {
  dataset <- list(provenance = list(warnings = list(
    list(code = "SOME_OTHER_CODE", message = "z", details = NULL)
  )))
  testthat::expect_true(is.na(extract_drift_flag(dataset)))
})
