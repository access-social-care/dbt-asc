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
##

LOADERS_DIR="/srv/projects/dbt-asc/loaders"
PROJECT_DIR="/srv/projects/dbt-asc"
LOG_DIR="/srv/projects/cc"
PIPELINE_START=$(date +%s)
FAILURES=0

# ── Stage 1: Load raw data ────────────────────────────────────────────────────

echo "=== Stage 1: Loaders starting at $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$LOADERS_DIR"

run_loader() {
    local name=$1
    local loader_start loader_end loader_diff exit_code

    loader_start=$(date +%s)
    echo "--- Starting $name at $(date '+%Y-%m-%d %H:%M:%S') ---"

    Rscript "${name}.R" > "${LOG_DIR}/${name}.log" 2>&1
    exit_code=$?

    loader_end=$(date +%s)
    loader_diff=$(( loader_end - loader_start ))

    if [ $exit_code -ne 0 ]; then
        echo "ERROR: $name failed (exit $exit_code) in ${loader_diff}s — see ${LOG_DIR}/${name}.log"
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
mkdir -p "$PROJECT_DIR/logs"

dbt build
DBT_EXIT=$?

if [ $DBT_EXIT -ne 0 ]; then
    PIPELINE_END=$(date +%s)
    PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))
    echo "ERROR: Stage 2 failed — dbt build exited $DBT_EXIT"
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (FAILED stage2)"
    exit 1
fi

echo "OK: dbt build completed"

# ── Stage 3: regenerate dbt docs ─────────────────────────────────────────────

echo "=== Stage 3: dbt docs generate at $(date '+%Y-%m-%d %H:%M:%S') ==="

dbt docs generate
DOCS_EXIT=$?

PIPELINE_END=$(date +%s)
PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))

if [ $DOCS_EXIT -ne 0 ]; then
    echo "WARN: dbt docs generate failed (exit $DOCS_EXIT) — build succeeded, docs may be stale"
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (docs FAILED)"
    exit 0  # docs failure is not a pipeline failure
fi

echo "OK: dbt docs regenerated"
