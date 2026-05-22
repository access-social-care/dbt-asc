#!/bin/bash
##
## Runs all Snowflake loaders in dependency order.
##
## Loaders are grouped by type:
##   Source system loads  — pull raw data from external systems (Monday.com, AdvicePro)
##   Derived/lookup loads — build from data already in Snowflake (postcode → geography, etc.)
##
## Add derived loaders after the source loads they depend on.
##
## Usage (manual):
##   bash loaders/load_primary_data.sh
## Usage (cron):
##   0 6 * * * /srv/projects/dbt-asc/loaders/load_primary_data.sh >> /srv/projects/cc/load_primary_data.timeRun.txt 2>&1
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

# ── Source system loads ──────────────────────────────────────────────────────

run_loader load_member_orgs_to_snowflake             # Monday.com → REFERENCE.MEMBER_ORGANISATIONS
run_loader load_advicepro_demographics_to_snowflake  # AdvicePro FD7DXGL4 → CASEWORK.ADVICEPRO_DEMOGRAPHICS

# ── Derived / lookup loads ───────────────────────────────────────────────────

run_loader load_casework_locality_to_snowflake       # case postcodes → findthatpostcode.uk → CASEWORK.CASEWORK_LOCALITY

# ─────────────────────────────────────────────────────────────────────────────

END=$(date +%s)
DIFF=$(( END - START ))

if [ $FAILURES -gt 0 ]; then
    echo "XXX load_primary_data $START $DIFF ($FAILURES loader(s) FAILED)"
    exit 1
else
    echo "XXX load_primary_data $START $DIFF"
fi
