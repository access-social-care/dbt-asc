#!/bin/bash
##
## Runs dbt build across all models.
##
## NOTE: For production cron, use run_pipeline.sh (repo root) instead.
##   run_pipeline.sh runs loaders then dbt as a single pipeline, so a loader
##   failure prevents dbt from running against stale data. This script is kept
##   for manual re-runs of the dbt stage only (e.g. fixing a model without
##   re-loading all raw data).
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
