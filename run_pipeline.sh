#!/bin/bash
##
## Daily pipeline: load raw data, run dbt transforms, export to S3 + Redis.
##

LOADERS_DIR="/srv/projects/dbt-asc/loaders"
PROJECT_DIR="/srv/projects/dbt-asc"
LOG_DIR="/srv/projects/dbt-asc/logs/"
FAILURES=0

# Load credentials (not stored in repo -- must exist on the VM)
# shellcheck source=/dev/null
source ~/.snowflake_env
# AWS + bastion creds for S3/Redis export (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
# AWS_DEFAULT_REGION, BASTION_KEY_PATH, BASTION_HOST, BASTION_USER, REDIS_HOST, REDIS_PORT)
# shellcheck source=/dev/null
source ~/.aws_asc_env

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
        echo "ERROR: $name failed (exit $exit_code) in ${loader_diff}s - see ${LOG_DIR}/${name}.log"
        FAILURES=$(( FAILURES + 1 ))
    else
        echo "OK: $name completed in ${loader_diff}s"
    fi
}

# Source system loads
run_loader load_member_orgs_to_snowflake             # Monday.com -> REFERENCE.MEMBER_ORGANISATIONS
run_loader load_advicepro_demographics_to_snowflake  # AdvicePro FD7DXGL4 -> CASEWORK.ADVICEPRO_DEMOGRAPHICS
run_loader load_external_sources_to_snowflake        # asc-agent data-portal CSVs -> REFERENCE.PUBLIC.<dataset_id>

# Derived / lookup loads (depend on source loads above)
run_loader load_casework_locality_to_snowflake       # case postcodes -> findthatpostcode.uk -> CASEWORK.CASEWORK_LOCALITY

if [ $FAILURES -gt 0 ]; then
    echo "ERROR: Stage 1 failed ($FAILURES loader(s) failed) - aborting pipeline"
    exit 1
fi

echo "OK: Stage 1 completed"

#  Stage 2: dbt build

echo "=== Stage 2: dbt build starting at $(date '+%Y-%m-%d %H:%M:%S') ==="

# dbt is installed via pip3; cron PATH is minimal so expand it explicitly
export PATH="$PATH:/home/amit/.local/bin:/usr/local/bin"

cd "$PROJECT_DIR"

source ~/.snowflake_env

~/.local/bin/dbt build
DBT_EXIT=$?

if [ $DBT_EXIT -ne 0 ]; then
    echo "ERROR: Stage 2 failed - dbt build exited $DBT_EXIT (see $PROJECT_DIR/logs/dbt_run.log)"
    exit 1
fi

echo "OK: dbt build completed"

#  Stage 3: export Gloucestershire mart tables -> S3 + Redis

echo "=== Stage 3: S3 + Redis export starting at $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$LOADERS_DIR"
Rscript export_la_queries_to_s3.R > "${LOG_DIR}/export_la_queries_to_s3.log" 2>&1
EXPORT_EXIT=$?

if [ $EXPORT_EXIT -ne 0 ]; then
    echo "ERROR: Stage 3 failed - export_la_queries_to_s3.R exited $EXPORT_EXIT (see ${LOG_DIR}/export_la_queries_to_s3.log)"
    exit 1
fi

echo "OK: S3 + Redis export completed"

#  Stage 4: regenerate dbt docs

echo "=== Stage 4: dbt docs generate at $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$PROJECT_DIR"
~/.local/bin/dbt docs generate
DOCS_EXIT=$?

if [ $DOCS_EXIT -ne 0 ]; then
    echo "WARN: dbt docs generate failed (exit $DOCS_EXIT) - build succeeded, docs may be stale"
    exit 0  # docs failure is not a pipeline failure
fi

echo "OK: dbt docs regenerated"

#  Stage 5: Observability
# Non-fatal - failure here does not affect pipeline exit code.
# a) dbt source freshness: data-level check on source tables (AVA, HELPLINES, CASEWORK)
# b) snowflake_staleness_check.R: INFORMATION_SCHEMA.LAST_ALTERED across source
#    databases - ETL-level check (did the pipeline actually run?).

echo "=== Stage 5: Observability at $(date '+%Y-%m-%d %H:%M:%S') ==="

echo "--- dbt source freshness ---"
~/.local/bin/dbt source freshness > "$LOG_DIR/source_freshness.log" 2>&1 || \
    echo "WARN: dbt source freshness exited non-zero - check $LOG_DIR/source_freshness.log"

echo "--- Snowflake staleness check (INFORMATION_SCHEMA) ---"
cd "$LOADERS_DIR"
Rscript snowflake_staleness_check.R > "$LOG_DIR/snowflake_staleness_check.log" 2>&1 || \
    echo "WARN: snowflake_staleness_check.R exited non-zero - check $LOG_DIR/snowflake_staleness_check.log"

echo "OK: Pipeline complete"
