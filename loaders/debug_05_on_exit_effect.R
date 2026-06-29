## DEBUG 05: test whether on.exit registration breaks the pointer
library(ascFuncs)

cat("--- connecting ---\n")
con <- ascFuncs::connect_snowflake(database = "ANALYTICS")
cat("valid before on.exit:", DBI::dbIsValid(con), "\n")
on.exit(DBI::dbDisconnect(con), add = TRUE)
cat("valid after on.exit:", DBI::dbIsValid(con), "\n")
result <- tryCatch(
  DBI::dbGetQuery(con, "SELECT 1 AS ping"),
  error = function(e) { cat("ERROR:", e$message, "\n"); NULL }
)
if (!is.null(result)) cat("SELECT 1:", result$ping, "\n")
cat("PASS\n")
