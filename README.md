# dbt-asc

dbt transformation layer for Access Social Care's Snowflake data warehouse. Combines raw data from three sources (chatbot, AdvicePro casework, helplines) into governed mart and staging tables for web products, Power BI, and reporting.

**Repo also contains the Snowflake loaders** (`loaders/`) — R scripts that pull from upstream APIs and write raw tables to Snowflake before dbt runs.

---

## Architecture

```mermaid
graph LR
    AP["AdvicePro API"]:::external
    MON["Monday.com API"]:::external
    FTPC["findthatpostcode.uk"]:::external
    CBD["chatbot_data repo"]:::external

    LPD["load_primary_data.sh<br>06:00 daily"]:::process
    LSV["load_synthetic_views.sh<br>06:30 daily"]:::process

    AVA["AVA.PUBLIC<br>ACCESSAVA"]:::source
    CW["CASEWORK.PUBLIC<br>ADVICEPRO_CASEWORK<br>ADVICEPRO_DEMOGRAPHICS<br>CASEWORK_LOCALITY"]:::source

    STG1["stg_advicepro"]:::product
    STG2["stg_la_queries"]:::product
    MART["35 LA product<br>mart models"]:::final
    ANA["ANALYTICS.PUBLIC<br>la_product schema"]:::final

    WEB["Power BI / web products"]:::external

    AP -->|"AdvicePro + demographics"| LPD
    MON -->|"member orgs"| LPD
    AP --> LSV
    FTPC -->|"postcode lookup"| LSV
    CBD -->|"daily ~05:00"| AVA

    LPD --> CW
    LPD --> AVA
    LSV --> CW

    AVA -->|"dbt run 08:30"| STG1
    CW --> STG1
    STG1 --> STG2
    AVA --> STG2
    STG2 --> MART
    MART --> ANA
    ANA --> WEB

    classDef external fill:#e0f2f1,stroke:#80cbc4
    classDef process fill:#e1f5ff,stroke:#81d4fa
    classDef source fill:#f3e5f5,stroke:#ce93d8
    classDef product fill:#fff4e1,stroke:#ffcc80
    classDef final fill:#e8f5e9,stroke:#a5d6a7
```

Three raw databases feed into dbt:

| Database | Schema | Loaded by | Schedule |
|---|---|---|---|
| `AVA` | `PUBLIC` | `chatbot_data` repo (`data_uploader.R`) | Daily ~05:00 |
| `CASEWORK` | `PUBLIC` | `loaders/load_primary_data.sh` + `load_synthetic_views.sh` (this repo) | Daily 06:00-06:30 |
| `HELPLINES` | `PUBLIC` | `helplines_data` repo | Daily ~05:00 |

dbt transforms all three into `ANALYTICS.PUBLIC` — the single schema consumed by web products and Power BI.

---

## Daily Pipeline (Cron)

```
06:00  load_primary_data.sh   — AdvicePro API + Monday.com → CASEWORK/AVA/REFERENCE tables
06:30  load_synthetic_views.sh — case postcodes → findthatpostcode.uk → CASEWORK_LOCALITY
```

**Crontab entries** (on the VM — edit with `crontab -e`):
```
0  6 * * * /srv/projects/dbt-asc/loaders/load_primary_data.sh >> /srv/projects/cc/load_primary_data.timeRun.txt 2>&1
30 6 * * * /srv/projects/dbt-asc/loaders/load_synthetic_views.sh >> /srv/projects/cc/load_synthetic_views.timeRun.txt 2>&1
```

---

## Repository Structure

```
dbt-asc/
├── dbt_project.yml           # Main project config (anonymous stats disabled)
├── packages.yml              # dbt package dependencies (dbt-utils)
│
├── dbt_pipeline.sh           # dbt runner (deps → run → test → docs generate)
│                             #   writes to logs/dbt_run.log (overwrites each run)
│
├── loaders/                  # R scripts: extract from APIs, load to Snowflake RAW
│   ├── load_primary_data.sh                      # CRONTAB 06:00 — source system loads
│   ├── load_synthetic_views.sh                   # CRONTAB 06:30 — derived/lookup loads
│   ├── load_advicepro_demographics_to_snowflake.R  # AdvicePro FD7DXGL4 → CASEWORK.ADVICEPRO_DEMOGRAPHICS
│   ├── load_casework_locality_to_snowflake.R       # AdvicePro PWVDK69X → CASEWORK.CASEWORK_LOCALITY
│   ├── load_member_orgs_to_snowflake.R             # Monday.com → REFERENCE.MEMBER_ORGANISATIONS
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
2. Add a `run_loader` call in `load_primary_data.sh` (source system) or `load_synthetic_views.sh` (derived/lookup)
3. Document the API report columns in `loaders/report_schemas.yml`
4. Add the target table to `models/sources.yml`

### report_schemas.yml

Schema registry for all AdvicePro API reports. Documents the mapping between raw UI column names (with spaces) and normalized column names (tolower + gsub), plus which script consumes each report and where it writes. This is the canonical reference when debugging column name errors.

---

## Models

### Staging — `models/staging/la_product/`

| Model | Description |
|---|---|
| `stg_advicepro.sql` | Stage 1 — joins `ADVICEPRO_CASEWORK` + `ADVICEPRO_DEMOGRAPHICS` + `CASEWORK_LOCALITY` into one row per case |
| `stg_la_queries.sql` | Stage 2 — UNION ALL of `stg_advicepro` and AccessAva. Single grain for all 35 mart models. Columns: `LA_NAME`, `QUERY_DATE`, `SOURCE_SYSTEM`, `QUERY_COUNT`, `SEGMENT`, `AGE_BAND`, `HAS_LETTER`, `LOCALITY_NAME` |

### Marts — `models/marts/`

| Model | Description |
|---|---|
| `mart_chatbot_*` (2 models) | Chatbot conversation counts by tenant (monthly + all-time) |
| `mart_la_{view}_{N}m` (35 models) | LA product views across 7 analytical angles × 5 time windows (1m, 3m, 6m, 9m, 12m) |

### Macros — `macros/la_product/`

Reusable SQL logic called by LA product mart models. Each macro takes `months_back` as a parameter and returns a filtered, aggregated, SDC-suppressed view.

```mermaid
graph TD
    CWORK["CASEWORK.PUBLIC<br>ADVICEPRO_CASEWORK"]:::source
    DEMO["CASEWORK.PUBLIC<br>ADVICEPRO_DEMOGRAPHICS"]:::source
    LOC["CASEWORK.PUBLIC<br>CASEWORK_LOCALITY"]:::source
    AAVA["AVA.PUBLIC<br>ACCESSAVA"]:::source
    AVLOC["AVA.PUBLIC<br>ACCESSAVA_LOCALITY"]:::source

    SADV["stg_advicepro<br>1 row per AdvicePro case"]:::product
    SLQ["stg_la_queries<br>UNION ALL — all sources"]:::product

    M1["la_activity_summary<br>la_queries_over_time"]:::process
    M2["la_locality_overview<br>la_query_source"]:::process
    M3["la_demographics<br>la_query_segments<br>la_legal_letters"]:::process
    SUPP["la_suppress()<br>counts < 5 → '1-5'"]:::process

    MART["35 mart models<br>mart_la_{view}_{N}m"]:::final

    CWORK --> SADV
    DEMO --> SADV
    LOC --> SADV
    AAVA --> SLQ
    AVLOC --> SLQ
    SADV --> SLQ
    SLQ --> M1
    SLQ --> M2
    SLQ --> M3
    M1 --> MART
    M2 --> MART
    M3 --> MART
    SUPP -.->|"applied in every macro"| MART

    classDef source fill:#f3e5f5,stroke:#ce93d8
    classDef product fill:#fff4e1,stroke:#ffcc80
    classDef process fill:#e1f5ff,stroke:#81d4fa
    classDef final fill:#e8f5e9,stroke:#a5d6a7
```

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
- **Runtime**: reads `dbt_run.timeRun.txt` written by `dbt_pipeline.sh`

If dbt fails, cc will open a GitHub issue in this repo automatically.

---

## Developer Access

For querying `ANALYTICS.PUBLIC` from Python, R, or BI tools, see:
- `setup/create_tenant_reports_user.sql` — creates `TENANT_REPORTS_USER` + `ROLE_TENANT_REPORTS_READ`
- `../admin/snowflake_developer_connection_guide.md` — connection setup and examples
