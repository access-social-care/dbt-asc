# dbt-asc Setup Guide

Complete setup instructions for deploying dbt transformation layer to ASC infrastructure.

## Prerequisites Checklist

- [ ] dbt-core and dbt-snowflake installed on VM (`pip3 install dbt-core dbt-snowflake`)
- [  ] VM has access to `/home/amit/.snowflake_env` with SNOWFLAKE_* environment variables
- [ ] Snowflake ACCOUNTADMIN access (for one-time permissions setup)
- [ ] Git access to create access-social-care/dbt-asc repo

## Step 1: Snowflake Infrastructure Setup

**Who**: Amit (or anyone with ACCOUNTADMIN role)  
**When**: Once, before first dbt run  
**Time**: ~5 minutes

```sql
-- Connect to Snowflake as ACCOUNTADMIN
-- Copy-paste contents of snowflake_permissions.sql
-- Or run: snowsql -f setup/snowflake_permissions.sql
```

**Verify**:
```sql
SHOW DATABASES;  -- Should see AVA
SHOW SCHEMAS IN DATABASE AVA;  -- Should see ACCESSAVA, ANALYTICS
SHOW GRANTS TO ROLE ROLE_DBT_TRANSFORM;  -- Should see permissions
SHOW GRANTS TO USER ETL_USER;  -- Should see ROLE_DBT_TRANSFORM granted
```

## Step 2: Create GitHub Repository

**Who**: Amit  
**When**: Once  
**Time**: ~2 minutes

```bash
# From local machine
cd "C:\Users\AmitKohli\Dropbox\My projects\_ASC\dbt-asc"
git init
git add .
git commit -m "Initial dbt-asc setup: two chatbot mart models"

# Create repo on GitHub
gh repo create access-social-care/dbt-asc --public --source=. --remote=origin
git push -u origin main
```

## Step 3: Clone to VM

**Who**: Amit (logged into VM)  
**When**: Once  
**Time**: ~2 minutes

```bash
ssh amit@data.accesscharity.org.uk

cd /srv/projects
git clone git@github.com:access-social-care/dbt-asc.git
cd dbt-asc

# Verify ownership
ls -la
# If owned by root, fix:
sudo chown -R amit:datausers /srv/projects/dbt-asc
```

## Step 4: Configure profiles.yml

**Who**: Amit (on VM)  
**When**: Once  
**Time**: ~3 minutes

```bash
# Create .dbt directory
mkdir -p ~/.dbt

# Copy template
cp /srv/projects/dbt-asc/setup/profiles.yml.template ~/.dbt/profiles.yml

# Verify environment variables are set
grep SNOWFLAKE ~/.snowflake_env
# Should show: SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE, etc.

# Test connection
cd /srv/projects/dbt-asc
dbt debug

# Expected output:
# All checks passed!
```

**Troubleshooting**:
- `dbt: command not found` → Run `pip3 install dbt-core dbt-snowflake`
- `Database 'AVA' does not exist` → Run Step 1 (Snowflake setup)
- `SNOWFLAKE_USER environment variable not set` → Source `.snowflake_env`: `source ~/.snowflake_env && dbt debug`

## Step 5: Install dbt Packages

**Who**: Amit (on VM)  
**When**: Once (and after updating packages.yml)  
**Time**: ~1 minute

```bash
cd /srv/projects/dbt-asc
dbt deps  # Installs dbt-utils
```

## Step 6: Verify Column Names in Snowflake

**Who**: Amit  
**When**: Before first dbt run  
**Time**: ~5 minutes

Current SQL models use placeholder column name `organisation_name` for tenant. Need to verify actual column.

```sql
-- Connect to Snowflake
SELECT * FROM AVA.ACCESSAVA.ACCESSAVA LIMIT 1;
```

**If column name is different**:
1. Edit `models/marts/mart_chatbot_conversations_by_tenant_monthly.sql`
2. Edit `models/marts/mart_chatbot_conversations_by_tenant_total.sql`
3. Replace `organisation_name` with actual column name
4. Remove `-- TODO: Verify column name` comments
5. Commit changes

## Step 7: Test dbt Run (Dry Run)

**Who**: Amit (on VM)  
**When**: After Steps 1-6 complete  
**Time**: ~2 minutes

```bash
cd /srv/projects/dbt-asc

# Show what dbt will do (doesn't execute)
dbt compile

# Run transformations
dbt run --target prod

# Expected output:
# Completed successfully
# 2 of 2 models OK created table...
```

**Verify tables created**:
```sql
-- In Snowflake
SHOW TABLES IN SCHEMA AVA.ANALYTICS;
-- Should see: MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY, MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL

SELECT * FROM AVA.ANALYTICS.MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY LIMIT 5;
-- Should return aggregated data
```

## Step 8: Run Tests

**Who**: Amit (on VM)  
**When**: After successful dbt run  
**Time**: ~1 minute

```bash
cd /srv/projects/dbt-asc
dbt test

# Expected output:
# X of X tests passed
# Tests cover: not_null, unique, unique_combination_of_columns
```

**If tests fail**:
- Check error message for which test failed
- Examine data: `SELECT * FROM AVA.ANALYTICS.[table_name] WHERE [failing_condition]`
- Fix source data issue or adjust test expectations

## Step 9: Generate Documentation

**Who**: Amit (optional - for team review)  
**When**: After successful run  
**Time**: ~2 minutes

```bash
cd /srv/projects/dbt-asc
dbt docs generate
dbt docs serve --port 8081

# Open browser to: http://data.accesscharity.org.uk:8081
# Browse lineage DAG, model descriptions
```

## Step 10: Add to Cron Schedule

**Who**: Amit (on VM)  
**When**: After successful manual test  
**Time**: ~3 minutes

```bash
# Edit crontab
crontab -e

# Add this line (runs daily at 05:30, after chatbot ETL at 05:00):
30 5 * * * cd /srv/projects/dbt-asc && source ~/.snowflake_env && dbt run --target prod >> /var/log/dbt_run.log 2>&1

# Save and exit

# Verify cron job was added
crontab -l | grep dbt

# Update cron backup
crontab -l > /srv/projects/cc/crontab_backup_$(date +\%Y\%m\%d).txt
```

**Monitor first automated run**:
```bash
# Next day, check log
tail -f /var/log/dbt_run.log

# Should see:
# Completed successfully
# 2 of 2 models OK created table...
```

## Step 11: Update Documentation

**Who**: Amit  
**When**: After production deployment  
**Time**: ~10 minutes

Update these files:

1. **admin/REPOSITORY_MAP.md**:
   ```markdown
   | dbt-asc | dbt transformation layer for Snowflake | Mart tables for web products | NEW |
   ```

2. **admin/snowflake_etl_strategy.md** - Add Phase 2 implementation section:
   ```markdown
   ## Phase 2: dbt Transformation Layer (COMPLETE)
   - Repo: dbt-asc
   - Schema: AVA.ANALYTICS
   - Status: Production (2 chatbot mart models)
   ```

3. **cc/README.md** - Document new cron job:
   ```markdown
   | 05:30 | dbt run | dbt-asc | Transform raw to analytics marts |
   ```

## Rollback Procedure

If dbt deployment causes issues:

```bash
# 1. Stop cron job
crontab -e
# Comment out dbt line, save

# 2. Drop analytics tables (if corrupted)
# In Snowflake:
DROP TABLE IF EXISTS AVA.ANALYTICS.MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY;
DROP TABLE IF EXISTS AVA.ANALYTICS.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL;

# 3. Revert repo if needed
cd /srv/projects/dbt-asc
git log  # Find last known good commit
git reset --hard [commit_hash]

# 4. Re-run from Step 7
```

## Next Steps

After successful deployment:

1. **Verify consumers can access**: Test Power BI connection to AVA.ANALYTICS.* tables
2. **Create GitHub issue for column name fix** (if placeholder used)
3. **Plan next mart models**: Casework aggregations, unified dimensions
4. **Document governance workflow**: PR template for business logic changes
5. **Consider dbt Cloud** (optional): UI for job monitoring, alerting, doc hosting

## Support

- **Questions**: Ask in #data Slack channel
- **Bugs**: Open issue in access-social-care/dbt-asc
- **dbt docs**: https://docs.getdbt.com/
- **Snowflake adapter docs**: https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
