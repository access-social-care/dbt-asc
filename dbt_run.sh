#!/bin/bash
START=$(date +%s)

# change path to working directory
cd /srv/projects/dbt-asc

# run dbt transformations and tests
dbt run --target prod > dbt_run.log 2>&1
dbt test >> dbt_run.log 2>&1

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "XXX dbt_run $START $DIFF"
