#!/bin/bash
##
## Stage 2 of the daily pipeline: run dbt build across all models.
##
## Pipeline order:
##   Stage 1 — loaders/load_primary_data.sh (06:00)  R loaders → raw Snowflake tables
##   Stage 2 — run_dbt.sh               (06:45)  dbt transforms → ANALYTICS schema
##
## Prerequisites:
##   - load_primary_data.sh must have completed successfully (raw tables must exist)
##   - dbt installed: pip install "dbt-snowflake>=1.7,<2.0"
##   - ~/.dbt/profiles.yml configured (see setup/profiles.yml.template)
##   - ~/.asc_secrets must exist with SNOWFLAKE_USER and SNOWFLAKE_KEY_FILE
##   - dbt packages installed: run `dbt deps` manually when packages.yml changes
##
## Usage (manual):
##   bash run_dbt.sh
## Usage (cron):
##   45 6 * * * /srv/projects/dbt-asc/run_dbt.sh >> /srv/projects/cc/run_dbt.timeRun.txt 2>&1
##

PROJECT_DIR="/srv/projects/dbt-asc"
START=$(date +%s)

# Load credentials (not stored in repo - must exist on the VM at ~/.asc_secrets)
# shellcheck source=/dev/null
source ~/.asc_secrets

cd "$PROJECT_DIR"

echo "--- dbt build starting at $(date '+%Y-%m-%d %H:%M:%S') ---"

dbt build
DBT_EXIT=$?

END=$(date +%s)
DIFF=$(( END - START ))

if [ $DBT_EXIT -ne 0 ]; then
    echo "ERROR: dbt build failed (exit $DBT_EXIT) in ${DIFF}s"
    echo "XXX run_dbt $START $DIFF (FAILED)"
    exit 1
else
    echo "OK: dbt build completed in ${DIFF}s"
    echo "XXX run_dbt $START $DIFF"
fi
