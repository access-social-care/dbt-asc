## DEBUG 04: connect AFTER all libs loaded (current script order)
library(ascFuncs)
library(tidyverse)
library(logger)
library(cli)
library(jsonlite)
library(processx)
library(redux)

cat("--- all libs loaded, now connecting ---\n")
con <- ascFuncs::connect_snowflake(database = "ANALYTICS")
cat("con valid:", DBI::dbIsValid(con), "\n")
result <- tryCatch(
  DBI::dbGetQuery(con, "SELECT 1 AS ping"),
  error = function(e) { cat("ERROR:", e$message, "\n"); NULL }
)
if (!is.null(result)) cat("SELECT 1:", result$ping, "\n")
DBI::dbDisconnect(con)
cat("PASS\n")
