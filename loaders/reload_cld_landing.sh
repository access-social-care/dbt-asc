#!/bin/bash
##
## One-off: reload the CLD landing tables into prod EXTERNAL_DATA after they
## have been emptied (runbook step 3 in loaders/sql/create_external_data_database.sql).
##
## Run on the VM, never a laptop: ascFuncs routes every ROLE_DEV write to the
## _DEV database (admin#5), so only the VM's prod role can land in EXTERNAL_DATA.
##
## Usage:
##   bash loaders/reload_cld_landing.sh <backfill_out_dir>
##
## <backfill_out_dir> holds <dataset_id>__<vintage>.csv for the six CLD
## datasets and both backfill vintages - the output of
## external_source_freshness_checker/scripts/backfill_cld_history.py
## (data/.backfill/out/, gitignored, so copy it to the VM first).
##
## The loader is manifest-driven, one CSV per dataset per run, so each vintage
## is staged into its own folder with its own manifest. Order: sep2025,
## dec2025, forced live run (current vintage), then dbt seed + build.
##
set -euo pipefail

BACKFILL_DIR="$(cd "${1:?usage: reload_cld_landing.sh <backfill_out_dir>}" && pwd)"
PROJECT_DIR="/srv/projects/dbt-asc"
LOADERS_DIR="$PROJECT_DIR/loaders"
EXTRACTOR_DIR="/srv/projects/external_source_freshness_checker"
STAGE_DIR="$(mktemp -d)"
LOG="$PROJECT_DIR/logs/reload_cld_landing.log"
export PATH="$PATH:/home/amit/.local/bin:/usr/local/bin"
: > "$LOG"

CLD_DATASETS="cld_long_term_support cld_long_term_support_gender cld_long_term_support_ethnicity
cld_assessments cld_assessments_gender cld_assessments_ethnicity"

step() { echo; echo "=== $1 ===" | tee -a "$LOG"; }

## The loader sources lib/*.R relative to cwd, so it must run from loaders/.
load() {
    step "loader: $1"
    ( cd "$LOADERS_DIR" && DATA_PORTAL_SOURCE_DIR="$2" Rscript load_external_sources_to_snowflake.R ) 2>&1 | tee -a "$LOG"
}

stage_vintage() {
    local vintage=$1 dir="$STAGE_DIR/$1" ids="" ds
    mkdir -p "$dir"
    for ds in $CLD_DATASETS; do
        cp "$BACKFILL_DIR/${ds}__${vintage}.csv" "$dir/${ds}.csv"
        ids="$ids{\"dataset_id\": \"$ds\", \"status\": \"SUCCESS\"},"
    done
    printf '{"run_id": "backfill-%s", "run_at": "2026-09-16T00:00:00Z", "datasets": [%s]}\n' \
        "$vintage" "${ids%,}" > "$dir/manifest.json"
    [ "$(ls "$dir"/*.csv | wc -l)" -eq 6 ] || { echo "expected 6 CSVs for $vintage" >&2; exit 1; }
}

source ~/.snowflake_env

for vintage in sep2025 dec2025; do
    stage_vintage "$vintage"
    load "$vintage" "$STAGE_DIR/$vintage"
done

step "checker: forced live run (current vintage)"
( cd "$EXTRACTOR_DIR" && "$EXTRACTOR_DIR/.venv/bin/source-checker" --force --out data run ) 2>&1 | tee -a "$LOG"
load live "$EXTRACTOR_DIR/data"

step "dbt seed + build"
( cd "$PROJECT_DIR" && dbt seed --select cld_published_population && dbt build --select staging.external normalised.external ) 2>&1 | tee -a "$LOG"

rm -rf "$STAGE_DIR"
echo
## A skip on either emptied table means it was not actually emptied - the
## loader's idempotency guard saw the old narrow vintage and exited 0.
if grep -iE 'already present in [A-Z_]+\.LANDING\.CLD_(ASSESSMENTS|LONG_TERM_SUPPORT) ' "$LOG"; then
    echo "WARNING: an emptied table was skipped - it was not actually emptied."
    exit 1
fi
echo "DONE. No skips on the emptied tables. Log: $LOG"
grep -m1 'Snowflake session: role=' "$LOG" || true
