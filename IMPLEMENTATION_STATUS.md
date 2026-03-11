# dbt-asc Implementation Status

**Date:** 2026-03-11  
**Session:** Initial setup with two chatbot mart models

## What's Been Completed

### 1. Repository Structure ✅
- Created `dbt-asc/` directory with standard dbt project layout
- All required folders: models/, marts/, staging/, seeds/, macros/, tests/, setup/
- GitHub repository created and pushed: `https://github.com/access-social-care/dbt-asc`

### 2. Core Configuration Files ✅
- **`dbt_project.yml`**: Project configuration with schema mappings
- **`packages.yml`**: dbt-utils dependency for advanced tests
- **`.gitignore`**: Properly excludes artifacts and credentials
- **`README.md`**: Comprehensive documentation following ASC standards with Mermaid diagram

### 3. Snowflake Setup Scripts ✅
- **`setup/snowflake_permissions.sql`**: Complete infrastructure setup
  - Creates AVA database + ANALYTICS schema
  - Creates ROLE_DBT_TRANSFORM with appropriate permissions
  - Grants to ETL_USER and ROLE_PBI_READ
  - Includes verification queries
- **`setup/profiles.yml.template`**: Connection config template for `~/.dbt/profiles.yml`
- **`setup/SETUP_GUIDE.md`**: Step-by-step deployment instructions (11 steps)

### 4. Source Definitions ✅
- **`models/sources.yml`**: Declares AVA.ACCESSAVA.ACCESSAVA as source
  - Includes freshness checks (warn after 36 hours)
  - Documents schema and data quality characteristics
  - Notes that ~15K conversations/year, demographics <5% complete

### 5. Mart Models (SQL) ✅
- **`models/marts/mart_chatbot_conversations_by_tenant_monthly.sql`**:
  - Aggregates conversations by tenant and month
  - Includes conversation counts, first/last dates
  - **Note**: Uses placeholder `organisation_name` - needs verification
  
- **`models/marts/mart_chatbot_conversations_by_tenant_total.sql`**:
  - All-time totals by tenant
  - Includes days_active calculation
  - **Note**: Uses placeholder `organisation_name` - needs verification

### 6. Model Documentation & Tests ✅
- **`models/marts/schema.yml`**: 
  - Column-level descriptions
  - not_null tests on key fields
  - unique tests on primary keys
  - unique_combination_of_columns test for composite keys
  - Business owner and approval workflow documentation

### 7. Admin Documentation ✅
- **`admin/REPOSITORY_MAP.md`**: Added dbt-asc entry to Core Infrastructure section
- Documents purpose, architecture, current models, dependencies

## What Needs to be Done (User)

### Prerequisites
- [ ] Install dbt on VM: `pip3 install dbt-core dbt-snowflake`
- [ ] Verify dbt installed: `dbt --version`

### Setup Steps (from setup/SETUP_GUIDE.md)

1. **Execute Snowflake setup** (5 min):
   ```bash
   # As ACCOUNTADMIN in Snowflake
   # Run contents of setup/snowflake_permissions.sql
   ```

2. **Clone to VM** (2 min):
   ```bash
   cd /srv/projects
   git clone git@github.com:access-social-care/dbt-asc.git
   sudo chown -R amit:datausers dbt-asc
   ```

3. **Configure profiles.yml** (3 min):
   ```bash
   mkdir -p ~/.dbt
   cp /srv/projects/dbt-asc/setup/profiles.yml.template ~/.dbt/profiles.yml
   # Verify: source ~/.snowflake_env && dbt debug
   ```

4. **Install packages** (1 min):
   ```bash
   cd /srv/projects/dbt-asc
   dbt deps  # Installs dbt-utils
   ```

5. **Column names verified** ✅:
   - Table location: `AVA.PUBLIC.ACCESSAVA` (not AVA.ACCESSAVA.ACCESSAVA)
   - Tenant column: `tenant_name` (correct in SQL models)
   - No SQL changes needed

6. **Test run** (2 min):
   ```bash
   dbt run --target prod
   dbt test
   ```

7. **Verify** (2 min):
   ```sql
   -- In Snowflake
   SELECT * FROM AVA.ANALYTICS.MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY LIMIT 5;
   SELECT * FROM AVA.ANALYTICS.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL LIMIT 5;
   ```

8. **Add to cron** (3 min):
   ```bash
   crontab -e
   # Add: 30 5 * * * cd /srv/projects/dbt-asc && source ~/.snowflake_env && dbt run --target prod >> /var/log/dbt_run.log 2>&1
   ```

9. **Update documentation** (10 min):
   - [ ] Update `admin/snowflake_etl_strategy.md` - add Phase 2 dbt section
   - [ ] Update `cc/README.md` - document new cron job

## Known Issues / TODOs

1. ~~**Column names not verified**~~ ✅ RESOLVED: Schema is AVA.PUBLIC (not AVA.ACCESSAVA), column is tenant_name

2. **AVA database may not exist**: Snowflake setup not confirmed
   - **Risk**: dbt debug will fail with "database does not exist"
   - **Fix**: Run setup/snowflake_permissions.sql as ACCOUNTADMIN

3. **No dev environment**: Only prod target configured
   - **Risk**: Local development modifies production tables
   - **Future**: Add ANALYTICS_DEV schema for safer testing

4. **No incremental models**: All models use full refresh (materialized='table')
   - **Impact**: Fine at current scale (~15K rows), won't scale to millions
   - **Future**: Switch to incremental models for performance

## Files Created

```
dbt-asc/
├── .gitignore                    # Excludes artifacts and credentials
├── README.md                     # Full documentation with Mermaid diagram
├── dbt_project.yml               # Main project config
├── packages.yml                  # dbt-utils dependency
├── models/
│   ├── sources.yml               # Raw Snowflake table definitions
│   └── marts/
│       ├── schema.yml            # Model docs and tests
│       ├── mart_chatbot_conversations_by_tenant_monthly.sql
│       └── mart_chatbot_conversations_by_tenant_total.sql
└── setup/
    ├── SETUP_GUIDE.md            # 11-step deployment guide
    ├── profiles.yml.template     # Connection config template
    └── snowflake_permissions.sql # Infrastructure setup SQL
```

## Next Session

After successful deployment, consider:
1. **Add casework mart models** (once advicePro_queries loading is stable)
2. **Create staging models** for standardization (LA name cleaning)
3. **Build cross-source models** (citizen journey = AccessAva + Casework)
4. **Add seeds** for reference data (member_name_mapping, ONS lookups)
5. **Create macros** for common patterns
6. **Implement incremental models** for performance
7. **Consider dbt Cloud** for UI, job monitoring, alerting

## References

- **GitHub**: https://github.com/access-social-care/dbt-asc
- **dbt docs**: https://docs.getdbt.com/
- **Snowflake adapter**: https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
- **Admin reference**: `admin/REPOSITORY_MAP.md`, `admin/snowflake_etl_strategy.md`
