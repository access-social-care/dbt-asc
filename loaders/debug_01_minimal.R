## DEBUG 01: ascFuncs only — baseline
## Expected: connects and queries successfully
## Run: Rscript loaders/debug_01_minimal.R

library(ascFuncs)

cat("--- connecting ---\n")
con <- ascFuncs::connect_snowflake(database = "ANALYTICS")
cat("con class:", paste(class(con), collapse = ", "), "\n")
cat("con valid:", DBI::dbIsValid(con), "\n")
result <- DBI::dbGetQuery(con, "SELECT 1 AS ping")
cat("SELECT 1:", result$ping, "\n")
DBI::dbDisconnect(con)
cat("PASS\n")
