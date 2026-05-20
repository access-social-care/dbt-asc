#!/bin/bash
# Wrapper: calls dbt_pipeline.sh and emits a clean timing line for cc.
# Crontab: 30 8 * * * /srv/projects/dbt-asc/run_dbt.sh >> /srv/projects/cc/dbt_run.timeRun.txt 2>&1
START=$(date +%s)

mkdir -p /srv/projects/dbt-asc/logs
/srv/projects/dbt-asc/dbt_pipeline.sh

END=$(date +%s)
DIFF=$(( END - START ))
echo "XXX dbt_run $START $DIFF"
