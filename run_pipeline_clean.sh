
START=$(date +%s)

PROJECT_DIR="/srv/projects/dbt-asc"

cd "$PROJECT_DIR"

./run_pipeline.sh > logs/overall_run.log

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "XXX dbt_pipeline $START $DIFF"


