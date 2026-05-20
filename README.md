# dbt-asc

dbt transformation layer for Access Social Care's Snowflake data warehouse. Combines raw data from three sources (chatbot, AdvicePro casework, helplines) into governed mart and staging tables for web products, Power BI, and reporting.

**Repo also contains the Snowflake loaders** (`loaders/`) — R scripts that pull from upstream APIs and write raw tables to Snowflake before dbt runs.

---

## Architecture

```
APIs / DBs  →  loaders/ (R)  →  Snowflake RAW  →  dbt models  →  ANALYTICS.PUBLIC  →  consumers
```

Three raw databases feed into dbt:

| Database | Schema | Loaded by | Schedule |
|---|---|---|---|
| `AVA` | `PUBLIC` | `chatbot_data` repo (`data_uploader.R`) | Daily ~05:00 |
| `CASEWORK` | `PUBLIC` | `loaders/run_all_loaders.sh` (this repo) | Daily 06:00 |
| `HELPLINES` | `PUBLIC` | `helplines_data` repo | Daily ~05:00 |

dbt transforms all three into `ANALYTICS.PUBLIC` — the single schema consumed by web products and Power BI.

---

## Daily Pipeline (Cron)

```
06:00  run_all_loaders.sh   — pull from AdvicePro API → write CASEWORK.PUBLIC tables
08:30  run_dbt.sh           — dbt deps + run + test + docs generate → ANALYTICS.PUBLIC
```

The 2.5-hour gap ensures loaders finish before dbt starts.

**Crontab entries** (on the VM — edit with `crontab -e`):
```
0 6 * * * /srv/projects/dbt-asc/loaders/run_all_loaders.sh >> /srv/projects/cc/run_all_loaders.timeRun.txt 2>&1
30 8 * * * /srv/projects/dbt-asc/run_dbt.sh >> /srv/projects/cc/dbt_run.timeRun.txt 2>&1
```

---

## Repository Structure

```
dbt-asc/
├── dbt_project.yml           # Main project config (anonymous stats disabled)
├── packages.yml              # dbt package dependencies (dbt-utils)
│
├── run_dbt.sh                # CRONTAB ENTRY — times dbt_pipeline.sh, outputs to cc
├── dbt_pipeline.sh           # Pure dbt runner (deps → run → test → docs generate)
│                             #   writes to logs/dbt_run.log (overwrites each run)
│
├── loaders/                  # R scripts: extract from APIs, load to Snowflake RAW
│   ├── run_all_loaders.sh    # CRONTAB ENTRY — master loader, runs all below in order
│   ├── load_casework_locality_to_snowflake.R   # AdvicePro report PWVDK69X → CASEWORK.PUBLIC.CASEWORK_LOCALITY
│   ├── load_member_orgs_to_snowflake.R         # Member org reference data → CASEWORK.PUBLIC
│   └── report_schemas.yml   # Schema registry: raw API column names → normalized names → target table
│
├── models/
│   ├── sources.yml           # dbt source declarations for all raw Snowflake tables
│   ├── staging/
│   │   └── la_product/
│   │       └── stg_la_queries.sql    # One row per interaction across all three sources
│   └── marts/
│       └── chatbot/
│           ├── mart_chatbot_conversations_by_tenant_monthly.sql
│           └── mart_chatbot_conversations_by_tenant_total.sql
│
├── macros/
│   └── la_product/           # Reusable SQL logic for LA product models
│       ├── la_activity_summary.sql
│       ├── la_demographics.sql
│       ├── la_legal_letters.sql
│       ├── la_locality_overview.sql
│       ├── la_queries_over_time.sql
│       ├── la_query_segments.sql
│       ├── la_query_source.sql
│       └── la_suppress.sql
│
├── logs/                     # Runtime logs (git-ignored)
│   └── dbt_run.log           # Full dbt output, overwritten each run
│
└── setup/
    ├── profiles.yml.template
    └── snowflake_permissions.sql
```

---

## Installation

### Prerequisites

- **dbt-core** >= 1.7.0
- **dbt-snowflake** adapter >= 1.7.0
- **Python** 3.8+ (for dbt)
- **R** with `ascFuncs`, `logger`, `DBI`, `httr` packages (for loaders)
- **Snowflake access**: credentials in `~/.asc_secrets` on the VM

### Install dbt

```bash
pip3 install dbt-core dbt-snowflake
dbt --version
```

### Configure connection

```bash
mkdir -p ~/.dbt
cp setup/profiles.yml.template ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with Snowflake user/key path
dbt debug   # Verify connection
```

Credentials are sourced from `~/.asc_secrets` (same file as R ETL jobs). Required variables: `SNOWFLAKE_USER`, `SNOWFLAKE_KEY_FILE`.

### Install dbt packages

```bash
dbt deps
```

### One-time Snowflake setup

Run `setup/snowflake_permissions.sql` as ACCOUNTADMIN to create the ANALYTICS database, roles, and grants.

Also grant schema creation to the ETL role:
```sql
GRANT CREATE SCHEMA ON DATABASE ANALYTICS TO ROLE ROLE_ETL_WRITE;
```

---

## Loaders

R scripts in `loaders/` extract from upstream APIs and write raw tables to Snowflake before dbt runs. All loaders are driven by `run_all_loaders.sh`.

### Adding a new loader

1. Create `loaders/load_{name}_to_snowflake.R`
2. Add the loader name to the `run_loader` calls in `run_all_loaders.sh`
3. Document the API report columns in `loaders/report_schemas.yml`
4. Add the target table to `models/sources.yml`

### report_schemas.yml

Schema registry for all AdvicePro API reports. Documents the mapping between raw UI column names (with spaces) and normalized column names (tolower + gsub), plus which script consumes each report and where it writes. This is the canonical reference when debugging column name errors.

---

## Models

### Staging — `models/staging/la_product/`

| Model | Description |
|---|---|
| `stg_la_queries.sql` | One row per interaction, unioned across AccessAva (chatbot), AdvicePro (casework), and Helplines. Columns: `LA_NAME`, `QUERY_DATE`, `SOURCE_SYSTEM`, `QUERY_COUNT`, `LOCALITY_NAME` (casework only). |

### Marts — `models/marts/`

| Model | Description |
|---|---|
| `mart_chatbot_conversations_by_tenant_monthly` | Monthly conversation counts per chatbot tenant |
| `mart_chatbot_conversations_by_tenant_total` | All-time conversation totals per chatbot tenant |

### Macros — `macros/la_product/`

Reusable SQL logic called by LA product models. Each macro encapsulates a specific analytical pattern (activity summary, demographics, locality overview, SDC suppression, etc.).

---

## Outputs

All models write to `ANALYTICS.PUBLIC` in Snowflake. Web products and Power BI connect to this schema only — never directly to AVA, CASEWORK, or HELPLINES.

### dbt Docs

`dbt docs generate` runs daily as part of `dbt_pipeline.sh`. Output lands in `target/`. To serve:

```nginx
# nginx config (add to existing server block on VM)
location /dbt-docs/ {
    alias /srv/projects/dbt-asc/target/;
    index index.html;
}
```

---

## Monitoring (Command Centre)

The cc dashboard at `data.accesscharity.org.uk/cc.html` monitors this repo:

- **Errors**: scans `logs/dbt_run.log` for ERROR lines (dbt's internal `logs/dbt.log` is excluded — too verbose)
- **Runtime**: reads `dbt_run.timeRun.txt` populated by `run_dbt.sh`

If dbt fails, cc will open a GitHub issue in this repo automatically.

---

## Developer Access

For querying `ANALYTICS.PUBLIC` from Python, R, or BI tools, see:
- `setup/create_tenant_reports_user.sql` — creates `TENANT_REPORTS_USER` + `ROLE_TENANT_REPORTS_READ`
- `../admin/snowflake_developer_connection_guide.md` — connection setup and examples
