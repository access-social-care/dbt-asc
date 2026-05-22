#!/bin/bash
##
## Daily pipeline: load raw data, then run dbt transforms.
##
## This is the single production runner for the full pipeline.
##
##   Stage 1 — R loaders push raw data from source systems into Snowflake
##             (REFERENCE, CASEWORK, AVA schemas)
##   Stage 2 — dbt build transforms raw tables into analytics models
##             (ANALYTICS schema)
##
## dbt only runs if ALL loaders succeed. A loader failure aborts the pipeline
## before any transforms run — no silent stale data.
##
## Usage (manual):
##   bash run_pipeline.sh
## Usage (cron):
##   0 6 * * * /srv/projects/dbt-asc/run_pipeline.sh >> /srv/projects/cc/run_pipeline.timeRun.txt 2>&1
##
## For manual re-runs of individual stages:
##   bash loaders/load_primary_data.sh   (loaders only)
##   bash run_dbt.sh                     (dbt only — assumes raw tables are already fresh)
##

LOADERS_DIR="/srv/projects/dbt-asc/loaders"
PROJECT_DIR="/srv/projects/dbt-asc"
PIPELINE_START=$(date +%s)
FAILURES=0

# Load credentials (not stored in repo — must exist on the VM at ~/.asc_secrets)
# shellcheck source=/dev/null
source ~/.asc_secrets

# ── Stage 1: Load raw data ────────────────────────────────────────────────────

echo "=== Stage 1: Loaders starting at $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$LOADERS_DIR"

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
        echo "ERROR: $name failed (exit $exit_code) in ${loader_diff}s — see ${LOADERS_DIR}/${name}.log"
        FAILURES=$(( FAILURES + 1 ))
    else
        echo "OK: $name completed in ${loader_diff}s"
    fi
}

# Source system loads
run_loader load_member_orgs_to_snowflake             # Monday.com → REFERENCE.MEMBER_ORGANISATIONS
run_loader load_advicepro_demographics_to_snowflake  # AdvicePro FD7DXGL4 → CASEWORK.ADVICEPRO_DEMOGRAPHICS

# Derived / lookup loads (depend on source loads above)
run_loader load_casework_locality_to_snowflake       # case postcodes → findthatpostcode.uk → CASEWORK.CASEWORK_LOCALITY

STAGE1_END=$(date +%s)
STAGE1_DIFF=$(( STAGE1_END - PIPELINE_START ))

if [ $FAILURES -gt 0 ]; then
    echo "ERROR: Stage 1 failed ($FAILURES loader(s) failed in ${STAGE1_DIFF}s) — aborting pipeline"
    PIPELINE_END=$(date +%s)
    PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (FAILED stage1)"
    exit 1
fi

echo "OK: Stage 1 completed in ${STAGE1_DIFF}s"

# ── Stage 2: dbt build ────────────────────────────────────────────────────────

echo "=== Stage 2: dbt build starting at $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$PROJECT_DIR"

dbt build
DBT_EXIT=$?

PIPELINE_END=$(date +%s)
PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))

if [ $DBT_EXIT -ne 0 ]; then
    echo "ERROR: Stage 2 failed — dbt build exited $DBT_EXIT"
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (FAILED stage2)"
    exit 1
fi

echo "OK: Stage 2 completed"
echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF"
