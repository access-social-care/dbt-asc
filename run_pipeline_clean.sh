
START=$(date +%s)

PROJECT_DIR="/srv/projects/dbt-asc"

cd "$PROJECT_DIR"

# Sync with GitHub before running — ensures cron always runs the latest code
echo "=== Syncing repo from GitHub at $(date '+%Y-%m-%d %H:%M:%S') ==="
git fetch origin
git reset --hard origin/main
echo "Now at commit: $(git rev-parse --short HEAD)"

./run_pipeline.sh > logs/overall_run.log

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "XXX dbt_pipeline $START $DIFF"


