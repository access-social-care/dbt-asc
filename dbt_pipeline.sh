#!/bin/bash
# Runs dbt pipeline. All output overwrites logs/dbt_run.log — do not call directly from cron.
# Use run_dbt.sh wrapper instead (handles timing + cc timeRun output).
cd /srv/projects/dbt-asc

# pull latest from main before running
git pull --rebase origin main

dbt deps  > logs/dbt_run.log 2>&1
dbt run --target prod >> logs/dbt_run.log 2>&1
dbt test  >> logs/dbt_run.log 2>&1
dbt docs generate >> logs/dbt_run.log 2>&1
