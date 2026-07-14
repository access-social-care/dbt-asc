## Per-dataset provenance helper, shared by load_external_sources_to_
## snowflake.R and its testthat coverage (tests/testthat/test-extract_drift_
## flag.R). Pulled out of the loader script so it can be sourced and tested
## without connecting to Snowflake or reading manifest.json.
##
## Drift codes are recorded per-source in provenance$warnings (a list of
## {code, message, details}); a dataset can have >1 source (multi-file
## merge).

`%||%` <- function(x, y) if (is.null(x)) y else x

extract_drift_flag <- function(dataset) {
  all_warnings <- dataset$provenance$warnings
  codes <- character(0)
  if (!is.null(all_warnings)) {
    codes <- c(
      codes,
      vapply(all_warnings, function(w) w$code %||% "", character(1))
    )
  }
  drift_codes <- unique(
    codes[codes %in% c("SCHEMA_DRIFT", "SEMANTIC_DRIFT_SUSPECTED")]
  )
  if (length(drift_codes) == 0) NA_character_ else paste(
    drift_codes, collapse = "+"
  )
}
