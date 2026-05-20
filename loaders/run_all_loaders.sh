#!/bin/bash
##
## Master loader — runs all dbt-asc loaders in dependency order.
##
## Add new loaders here as they are created. Each R script writes its own
## .log file; this script echoes overall timing for the cc dashboard.
##
## Usage (manual):
##   bash loaders/run_all_loaders.sh
## Usage (cron):
##   0 6 * * * /srv/projects/dbt-asc/loaders/run_all_loaders.sh >> /srv/projects/cc/run_all_loaders.timeRun.txt 2>&1
##

SCRIPT_DIR="/srv/projects/dbt-asc/loaders"
START=$(date +%s)
FAILURES=0

# Load credentials (not stored in repo — must exist on the VM at ~/.asc_secrets)
# shellcheck source=/dev/null
source ~/.asc_secrets

cd "$SCRIPT_DIR"

run_loader() {
    local name=$1
    local loader_start loader_end loader_diff exit_code

    loader_start=$(date +%s)
    echo "--- Starting $name at $(date '+%Y-%m-%d %H:%M:%S') ---"

    Rscript "${name}.R" > "${name}.log" 2>&1
    exit_code=$?

    loader_end=$(date +%s)
    loader_diff=$(( loader_end - loader_start ))

    if [ $exit_code -ne 0 ]; then
        echo "ERROR: $name failed (exit $exit_code) in ${loader_diff}s — see ${SCRIPT_DIR}/${name}.log"
        FAILURES=$(( FAILURES + 1 ))
    else
        echo "OK: $name completed in ${loader_diff}s"
    fi
}

# ── Loaders (add new ones below in dependency order) ─────────────────────────

run_loader load_member_orgs_to_snowflake
run_loader load_casework_locality_to_snowflake

# ─────────────────────────────────────────────────────────────────────────────

END=$(date +%s)
DIFF=$(( END - START ))

if [ $FAILURES -gt 0 ]; then
    echo "XXX run_all_loaders $START $DIFF ($FAILURES loader(s) FAILED)"
    exit 1
else
    echo "XXX run_all_loaders $START $DIFF"
fi
