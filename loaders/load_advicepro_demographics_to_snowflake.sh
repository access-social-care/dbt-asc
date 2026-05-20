#!/bin/bash
START=$(date +%s)

# load credentials (not stored in repo — must exist on the VM at ~/.asc_secrets)
# shellcheck source=/dev/null
source ~/.asc_secrets

# change path to working directory
cd /srv/projects/dbt-asc/loaders

Rscript load_advicepro_demographics_to_snowflake.R > load_advicepro_demographics_to_snowflake.log 2>&1

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "XXX load_advicepro_demographics_to_snowflake $START $DIFF"
