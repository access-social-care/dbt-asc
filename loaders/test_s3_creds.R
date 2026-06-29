## Minimal credential swap + S3 write test
## Run: Rscript loaders/test_s3_creds.R
## Writes a tiny sentinel file to each bucket, reports which key is active

if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3", repos = "https://cloud.r-project.org")

swap_aws_creds <- function(profile) {
  cred_file <- path.expand(paste0("~/.aws/", profile))
  if (!file.exists(cred_file)) stop(paste("~/.aws/", profile, "not found"), call. = FALSE)
  lines  <- readLines(cred_file)
  key_id <- trimws(sub(".*=\\s*", "", grep("aws_access_key_id",     lines, value = TRUE, ignore.case = TRUE)[1]))
  secret <- trimws(sub(".*=\\s*", "", grep("aws_secret_access_key", lines, value = TRUE, ignore.case = TRUE)[1]))
  Sys.setenv(AWS_ACCESS_KEY_ID = key_id, AWS_SECRET_ACCESS_KEY = secret)
  key_id
}

targets <- list(
  list(profile = "dev",  bucket = "asc-analytics-dashboard-backend-development-data"),
  list(profile = "prod", bucket = "asc-analytics-dashboard-backend-production-data")
)

tmp <- tempfile(fileext = ".json")
writeLines('{"test":true}', tmp)

for (t in targets) {
  key <- swap_aws_creds(t$profile)
  cat(sprintf("\n[%s] bucket: %s\n", t$profile, t$bucket))
  cat(sprintf("  key_id: %s\n", key))

  result <- tryCatch(
    {
      aws.s3::put_object(
        file   = tmp,
        object = "gloucestershire/_test_sentinel.json",
        bucket = t$bucket,
        region = Sys.getenv("AWS_DEFAULT_REGION", "eu-west-2")
      )
      "OK"
    },
    error = function(e) paste("FAILED:", conditionMessage(e))
  )
  cat(sprintf("  upload: %s\n", result))
}

unlink(tmp)
