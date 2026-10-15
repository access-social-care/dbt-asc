## Export slim CSVs of the R pipeline's own cached tr / tr_ev (chatbot_process_tr /
## chatbot_process_tr_ev output) for scripts/parity/chatbot_base_parity.py.
## Read-only on the caches. Only ids, timestamps, tenant, entry_* and lengths leave R.
##
## Usage: Rscript scripts/parity/export_r_cache.R <cache_dir> <out_dir>
##   cache_dir  e.g. C:/o/_ASC/data/chatbot (holds actions_transcript.RDS and
##              actions_transcript_events.RDS)
args <- commandArgs(trailingOnly = TRUE)
cache <- args[1]
out <- args[2]

x <- readRDS(file.path(cache, "actions_transcript.RDS"))
d <- data.frame(
  id = x$id,
  created_at = format(x$created_at, "%Y-%m-%d %H:%M:%OS6", tz = "UTC"),
  entry_day = as.character(x$entry_day), entry_month = x$entry_month,
  entry_week = x$entry_week, tenant_name = x$tenant_name,
  rtr_first = substr(x$raw_tracker_record, 1, 2)
)
write.csv(d, file.path(out, "r_tr.csv"), row.names = FALSE, fileEncoding = "UTF-8")
x <- NULL
d <- NULL
invisible(gc())

y <- readRDS(file.path(cache, "actions_transcript_events.RDS"))
d <- data.frame(
  id = y$id, transcript_id = y$transcript_id,
  created_at = format(y$created_at, "%Y-%m-%d %H:%M:%OS6", tz = "UTC"),
  bot_or_user = y$bot_or_user, entry_day = as.character(y$entry_day),
  entry_month = y$entry_month, entry_week = y$entry_week,
  contentful_id = y$contentful_id, lettercode = y$letterCode,
  tenant_name = y$tenant_name
)
write.csv(d, file.path(out, "r_trev.csv"), row.names = FALSE, fileEncoding = "UTF-8")
