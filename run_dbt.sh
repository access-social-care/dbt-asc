#!/bin/bash
# Wrapper: calls dbt_pipeline.sh and emits a clean timing line for cc.
# Exit code from dbt_pipeline.sh is propagated so cc can detect dbt failures.
# Crontab: 30 8 * * * /srv/projects/dbt-asc/run_dbt.sh >> /srv/projects/cc/dbt_run.timeRun.txt 2>&1
START=$(date +%s)

mkdir -p /srv/projects/dbt-asc/logs
/srv/projects/dbt-asc/dbt_pipeline.sh
DBT_EXIT=$?

END=$(date +%s)
DIFF=$(( END - START ))

if [ $DBT_EXIT -ne 0 ]; then
    echo "XXX dbt_run $START $DIFF (FAILED — exit $DBT_EXIT, see logs/dbt_run.log)"
    exit 1
else
    echo "XXX dbt_run $START $DIFF"
fi
