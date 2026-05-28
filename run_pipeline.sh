#!/bin/bash
##
## Daily pipeline: load raw data, then run dbt transforms.
##

LOADERS_DIR="/srv/projects/dbt-asc/loaders"
PROJECT_DIR="/srv/projects/dbt-asc"
LOG_DIR="/srv/projects/dbt-asc/logs/"
PIPELINE_START=$(date +%s)
FAILURES=0

# Load credentials (not stored in repo â€” must exist on the VM at ~/.asc_secrets)
# shellcheck source=/dev/null
source ~/.snowflake_env

#  Stage 1: Load raw data 

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
        echo "ERROR: $name failed (exit $exit_code) in ${loader_diff}s â€” see ${LOG_DIR}/${name}.log"
        FAILURES=$(( FAILURES + 1 ))
    else
        echo "OK: $name completed in ${loader_diff}s"
    fi
}

# Source system loads
run_loader load_member_orgs_to_snowflake             # Monday.com â†’ REFERENCE.MEMBER_ORGANISATIONS
run_loader load_advicepro_demographics_to_snowflake  # AdvicePro FD7DXGL4 â†’ CASEWORK.ADVICEPRO_DEMOGRAPHICS

# Derived / lookup loads (depend on source loads above)
run_loader load_casework_locality_to_snowflake       # case postcodes â†’ findthatpostcode.uk â†’ CASEWORK.CASEWORK_LOCALITY

STAGE1_END=$(date +%s)
STAGE1_DIFF=$(( STAGE1_END - PIPELINE_START ))

if [ $FAILURES -gt 0 ]; then
    echo "ERROR: Stage 1 failed ($FAILURES loader(s) failed in ${STAGE1_DIFF}s) â€” aborting pipeline"
    PIPELINE_END=$(date +%s)
    PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (FAILED stage1)"
    exit 1
fi

echo "OK: Stage 1 completed in ${STAGE1_DIFF}s"

#  Stage 2: dbt build 

echo "=== Stage 2: dbt build starting at $(date '+%Y-%m-%d %H:%M:%S') ==="

# dbt is installed via pip3; cron PATH is minimal so expand it explicitly
export PATH="$PATH:/home/amit/.local/bin:/usr/local/bin"

cd "$PROJECT_DIR"
# mkdir -p "$PROJECT_DIR/logs"

source ~/.snowflake_env

~/.local/bin/dbt build
DBT_EXIT=$?

if [ $DBT_EXIT -ne 0 ]; then
    PIPELINE_END=$(date +%s)
    PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))
    echo "ERROR: Stage 2 failed â€” dbt build exited $DBT_EXIT (see $PROJECT_DIR/logs/dbt_run.log)"
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (FAILED stage2)"
    exit 1
fi

echo "OK: dbt build completed"

#  Stage 3: regenerate dbt docs 

echo "=== Stage 3: dbt docs generate at $(date '+%Y-%m-%d %H:%M:%S') ==="

~/.local/bin/dbt docs generate
DOCS_EXIT=$?

PIPELINE_END=$(date +%s)
PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))

if [ $DOCS_EXIT -ne 0 ]; then
    echo "WARN: dbt docs generate failed (exit $DOCS_EXIT) â€” build succeeded, docs may be stale"
    echo "XXX run_pipeline $PIPELINE_START $PIPELINE_DIFF (docs FAILED)"
    exit 0  # docs failure is not a pipeline failure
fi

echo "OK: dbt docs regenerated"

#  Stage 4: Observability 
# Non-fatal â€” failure here does not affect pipeline exit code.
# a) dbt source freshness: data-level check on source tables (AVA, HELPLINES, CASEWORK)
# b) snowflake_staleness_check.R: INFORMATION_SCHEMA.LAST_ALTERED across source
#    databases â€” ETL-level check (did the pipeline actually run?).

echo "=== Stage 4: Observability at $(date '+%Y-%m-%d %H:%M:%S') ==="

echo "--- dbt source freshness ---"
~/.local/bin/dbt source freshness > "$PROJECT_DIR/logs/source_freshness.log" 2>&1 || \
    echo "WARN: dbt source freshness exited non-zero â€” check $PROJECT_DIR/logs/source_freshness.log"

echo "--- Snowflake staleness check (INFORMATION_SCHEMA) ---"
cd "$LOADERS_DIR"
Rscript snowflake_staleness_check.R > "$LOG_DIR/snowflake_staleness_check.log" 2>&1 || \
    echo "WARN: snowflake_staleness_check.R exited non-zero â€” check $LOG_DIR/snowflake_staleness_check.log"

PIPELINE_END=$(date +%s)
PIPELINE_DIFF=$(( PIPELINE_END - PIPELINE_START ))
echo "OK: Stage 4 complete"
