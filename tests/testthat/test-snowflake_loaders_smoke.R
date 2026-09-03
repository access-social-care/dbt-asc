## Smoke tests for the 4 loaders migrated to ascFuncs::snowflake_write_table()'s
## `operation` param: load_advicepro_demographics_to_snowflake.R,
## load_casework_locality_to_snowflake.R, load_external_sources_to_snowflake.R
## and load_member_orgs_to_snowflake.R, all under loaders/.
##
## Real signature (installed ascFuncs 0.5.0.0/0.6.0.0):
##   snowflake_write_table(con, table_name, data, schema = "PUBLIC",
##                          database = ..., operation, grant_select = FALSE)
## `operation` has no default - every call site must supply one explicitly.
## These tests exist to catch a regression where a loader's `operation`
## argument is silently changed to the wrong value.
##
## Approach: source() each loader script with the DBI/ascFuncs/httr boundary
## mocked out via testthat::local_mocked_bindings(), which replaces the
## binding inside the target package's namespace for the life of the test -
## this works even though the loaders call functions namespace-qualified
## (ascFuncs::snowflake_write_table(), DBI::dbGetQuery(), etc.), since the
## lookup is resolved dynamically against the (temporarily patched)
## namespace at call time. Each loader is sourced with `local = TRUE` (i.e.
## into the test_that()'s own environment) so scripts never leak top-level
## objects (TARGET_TABLE, con, ...) into the global env or into each other.
##
## Run: Rscript -e "testthat::test_dir('tests/testthat')" from dbt-asc/ root.

loaders_dir <- normalizePath(file.path("..", "..", "loaders"))

## Shared fake Snowflake session-info result (CURRENT_ROLE()/CURRENT_
## DATABASE()/CURRENT_SCHEMA()/CURRENT_USER() - column names are literal
## SQL-call-syntax, as Snowflake/DBI returns them).
fake_session_info <- data.frame(
  `CURRENT_ROLE()`     = "ROLE_ETL",
  `CURRENT_DATABASE()` = "TEST_DB",
  `CURRENT_SCHEMA()`   = "PUBLIC",
  `CURRENT_USER()`     = "ETL_USER",
  check.names = FALSE
)

mock_connection <- structure(list(), class = "mock_dbi_connection")

## Records every ascFuncs::snowflake_write_table() call. Returns a mock
## function plus the (environment-backed) list of calls it appends to, so
## a test can assert on `calls` after sourcing the loader.
make_write_table_mock <- function() {
  calls <- list()
  mock <- function(con, table_name, data, schema = "PUBLIC",
                   database = Sys.getenv("SNOWFLAKE_DATABASE", "AVA"),
                   operation, grant_select = FALSE) {
    calls[[length(calls) + 1]] <<- list(
      table_name = table_name,
      operation  = operation,
      database   = database,
      schema     = schema,
      nrow       = nrow(data)
    )
    invisible(NULL)
  }
  list(mock = mock, calls = function() calls)
}

# load_advicepro_demographics_to_snowflake.R --------------------------------
##
## Full exercise: entire script runs end to end. The only external boundary
## is ascFuncs::query_advicepro_report() (AdvicePro API) - stubbed with a
## synthetic 2-row report matching the real report's raw column shape
## (space-separated headers, "[Not Specified]" placeholder, a duplicate
## case_reference differing only by case/whitespace) so the normalise/dedup
## logic actually runs, not just the write call.

testthat::test_that("advicepro_demographics: operation is overwrite", {
  wt <- make_write_table_mock()

  fake_report <- data.frame(
    `Case Reference` = c("CASE-001", " case-001 ", "CASE-002"),
    `Disability Type` = c("Physical", "Physical", "[Not Specified]"),
    check.names = FALSE
  )

  testthat::local_mocked_bindings(
    query_advicepro_report = function(report_key) fake_report,
    connect_snowflake      = function(...) mock_connection,
    snowflake_write_table  = wt$mock,
    .package = "ascFuncs"
  )
  testthat::local_mocked_bindings(
    dbGetQuery  = function(conn, statement, ...) fake_session_info,
    dbDisconnect = function(conn, ...) invisible(TRUE),
    .package = "DBI"
  )

  source(file.path(loaders_dir, "load_advicepro_demographics_to_snowflake.R"),
         local = TRUE)

  calls <- wt$calls()
  testthat::expect_length(calls, 1)
  testthat::expect_identical(calls[[1]]$operation, "overwrite")
  testthat::expect_identical(calls[[1]]$table_name, "ADVICEPRO_DEMOGRAPHICS")
  ## the near-duplicate case_reference (differs only by case/whitespace)
  ## should have been deduped away
  testthat::expect_identical(calls[[1]]$nrow, 2L)
})

# load_casework_locality_to_snowflake.R --------------------------------------
##
## Full exercise of the "table doesn't exist yet" branch only - that is the
## ONLY branch that calls ascFuncs::snowflake_write_table() (the existing-
## table branch appends via DBI::dbAppendTable() directly, never touches
## snowflake_write_table). Forced by making the "SELECT 1 FROM ... LIMIT 0"
## existence probe error (mocked DBI::dbGetQuery throws for that specific
## statement), which is exactly how the real script's own tryCatch treats a
## table that doesn't exist yet.
##
## httr::GET()/httr::content() are stubbed (no real network call to
## findthatpostcode.uk) and base::Sys.sleep() is stubbed to a no-op so the
## script's 5s per-postcode politeness delay doesn't slow down the test
## suite - the sleep call itself carries no logic worth exercising.

testthat::test_that("casework_locality: operation is create when absent", {
  wt <- make_write_table_mock()

  fake_report <- data.frame(
    `Case Reference` = c("CASE-101", "CASE-102"),
    `Postcode`        = c("AB1 2CD", "AB1 2CD"),
    check.names = FALSE
  )

  testthat::local_mocked_bindings(
    query_advicepro_report = function(report_key) fake_report,
    connect_snowflake      = function(...) mock_connection,
    snowflake_write_table  = wt$mock,
    .package = "ascFuncs"
  )
  testthat::local_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) {
      if (grepl("^SELECT 1 FROM", statement)) {
        stop("simulated: table does not exist")
      }
      fake_session_info
    },
    dbDisconnect = function(conn, ...) invisible(TRUE),
    .package = "DBI"
  )
  testthat::local_mocked_bindings(
    GET = function(url, ...) list(url = url),
    content = function(x, ...) {
      list(data = list(attributes = list(
        laua_name = "Test Council", laua = "E00000001"
      )))
    },
    .package = "httr"
  )
  testthat::local_mocked_bindings(
    Sys.sleep = function(...) invisible(NULL),
    .package = "base"
  )

  source(file.path(loaders_dir, "load_casework_locality_to_snowflake.R"),
         local = TRUE)

  calls <- wt$calls()
  testthat::expect_length(calls, 1)
  testthat::expect_identical(calls[[1]]$operation, "create")
  testthat::expect_identical(calls[[1]]$table_name, "CASEWORK_LOCALITY")
  ## both cases share one postcode - both rows still carried through
  testthat::expect_identical(calls[[1]]$nrow, 2L)
})

# load_external_sources_to_snowflake.R ---------------------------------------
##
## Full exercise using a synthetic SOURCE_DIR (temp dir with a manifest.json
## + one CSV) via the DATA_PORTAL_SOURCE_DIR env var override the script
## already supports for exactly this reason. One SUCCESS dataset drives the
## real read_csv() -> snowflake_write_table() path (including the
## extract_drift_flag() call it already has unit coverage for elsewhere).
## table_exists() is mocked to FALSE throughout (irrelevant for a SUCCESS
## dataset, which always loads regardless).

testthat::test_that("external_sources: writes with operation = overwrite", {
  wt <- make_write_table_mock()

  tmp_dir <- withr::local_tempdir()
  dataset_id <- "test_dataset"
  writeLines(
    "a,b\n1,x\n2,y\n",
    file.path(tmp_dir, paste0(dataset_id, ".csv"))
  )
  manifest <- list(
    run_id = "RUN123",
    run_at = "2026-09-02T00:00:00Z",
    datasets = list(list(
      dataset_id = dataset_id,
      status = "SUCCESS",
      provenance = list(warnings = NULL)
    ))
  )
  jsonlite::write_json(
    manifest, file.path(tmp_dir, "manifest.json"),
    auto_unbox = TRUE
  )
  withr::local_envvar(DATA_PORTAL_SOURCE_DIR = tmp_dir)

  testthat::local_mocked_bindings(
    connect_snowflake     = function(...) mock_connection,
    snowflake_write_table = wt$mock,
    .package = "ascFuncs"
  )
  testthat::local_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) {
      if (grepl("^SELECT 1 FROM", statement)) stop("simulated: table absent")
      fake_session_info
    },
    dbDisconnect = function(conn, ...) invisible(TRUE),
    .package = "DBI"
  )

  ## the script's own source("lib/extract_drift_flag.R") is relative to
  ## loaders/ - match its own stated working-directory assumption
  withr::local_dir(loaders_dir)

  source(file.path(loaders_dir, "load_external_sources_to_snowflake.R"),
         local = TRUE)

  calls <- wt$calls()
  testthat::expect_length(calls, 1)
  testthat::expect_identical(calls[[1]]$operation, "overwrite")
  testthat::expect_identical(calls[[1]]$table_name, dataset_id)
  testthat::expect_identical(calls[[1]]$nrow, 2L)
})

# load_member_orgs_to_snowflake.R --------------------------------------------
##
## Full exercise. The only external boundary is
## ascFuncs::query_monday_board() (Monday.com API) - stubbed with a
## synthetic board$df covering both an in-scope contract group row and an
## out-of-scope row, so the group_title filter is actually exercised, not
## just the write call.

testthat::test_that("member_orgs: writes with operation = overwrite", {
  wt <- make_write_table_mock()

  fake_board <- list(df = data.frame(
    item_name   = c("Org A", "Org B"),
    Service     = c("Advice Membership", "Casework Membership 1 day per week"),
    group_title = c(
      "Organisations with signed contract/agreement",
      "Some other group"
    ),
    stringsAsFactors = FALSE
  ))

  testthat::local_mocked_bindings(
    query_monday_board    = function(board_id, ...) fake_board,
    connect_snowflake     = function(...) mock_connection,
    snowflake_write_table = wt$mock,
    .package = "ascFuncs"
  )
  testthat::local_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) {
      if (grepl("^SELECT COUNT", statement)) {
        return(data.frame(N_ROWS = 1L))
      }
      fake_session_info
    },
    dbDisconnect = function(conn, ...) invisible(TRUE),
    .package = "DBI"
  )

  source(file.path(loaders_dir, "load_member_orgs_to_snowflake.R"),
         local = TRUE)

  calls <- wt$calls()
  testthat::expect_length(calls, 1)
  testthat::expect_identical(calls[[1]]$operation, "overwrite")
  testthat::expect_identical(calls[[1]]$table_name, "MEMBER_ORGANISATIONS")
  ## only the row in the signed-contract group should survive the filter
  testthat::expect_identical(calls[[1]]$nrow, 1L)
})
