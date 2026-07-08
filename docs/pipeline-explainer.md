# dbt-asc Pipeline: How it all works

A technical explainer for anyone approaching this repo without prior dbt experience.

---

## The one-sentence version

Raw data from three external systems (AdvicePro casework, AccessAva chatbot, Access Helplines) lands in Snowflake. dbt reads it, maps each source onto a shared Universal Theme taxonomy (UT1/UT2), and writes a set of final tables that Power BI, S3, Redis, and web products consume. This repo contains the dbt SQL (transforms) and some of the R loaders (raw data ingestion) — AccessAva and Helplines are loaded by their own separate repos (`chatbot_data`, `helplines_data`).

---

## Why dbt?

Before dbt, "transforming data in a warehouse" usually meant either:
- Writing SQL views that reference each other with no enforced order
- Writing a big script that runs `CREATE TABLE AS SELECT ...` statements in a carefully maintained sequence

Both approaches break down as soon as you have more than a handful of tables. You lose track of which table depends on what, tests live in spreadsheets nobody updates, and every column rename requires a grep across dozens of files.

dbt solves this with a few core ideas:
1. **Each `.sql` file is one table** - you write a `SELECT` statement, dbt runs it and materialises the result
2. **You declare dependencies with `ref()`** - instead of hardcoding `CASEWORK.PUBLIC.ADVICEPRO_CASEWORK`, you write `{{ ref('stg_advicepro') }}` and dbt figures out the order to run everything
3. **Tests are part of the project** - you declare `not_null`, `unique`, `accepted_values` tests in YAML; dbt runs them after every build
4. **One command does everything** - `dbt build` builds every table in the right order and runs every test

---

## The five-stage daily pipeline (`run_pipeline.sh`)

```
06:00  Stage 1: R loaders  → Snowflake (raw tables this repo owns: member orgs, AdvicePro, casework locality)
       Stage 2: dbt build  → Snowflake (staging/intermediate/mart schemas)
       Stage 3: S3 + Redis export (Gloucestershire mart tables only)
       Stage 4: dbt docs generate
       Stage 5: Observability (source freshness + Snowflake staleness check) — non-fatal
```

Each stage only runs if the previous one succeeded, **except** Stage 5, which is explicitly non-fatal (a docs or freshness-check failure doesn't fail the pipeline). If Stage 1 fails, dbt never runs against stale data.

### Stage 1 - Loaders this repo owns

| Script | What it does | Why the order matters |
|---|---|---|
| `load_member_orgs_to_snowflake.R` | Pulls Access Social Care member organisations from Monday.com → `REFERENCE.MEMBER_ORGANISATIONS` | No dependencies |
| `load_advicepro_demographics_to_snowflake.R` | Pulls all AdvicePro casework records from the API → `CASEWORK.ADVICEPRO_DEMOGRAPHICS` | No dependencies |
| `load_casework_locality_to_snowflake.R` | Reads case postcodes written by the demographics loader, looks each one up via [findthatpostcode.uk](https://findthatpostcode.uk), appends new rows (LA name, county, ward) to `CASEWORK.CASEWORK_LOCALITY` | **Must run after demographics** - it reads postcodes from the table written in step 2 |

**Why the postcode lookup?** AdvicePro stores the client's postcode, not their local authority or county. There is no geography field in the raw AdvicePro API response. The locality loader is the bridge, and it's incremental — it only looks up postcodes for cases not already in `CASEWORK_LOCALITY`.

**AccessAva and Helplines are loaded by other repos, not this one.** AccessAva's `accessava` and `accessava_locality` tables are loaded by `chatbot_data/data_uploader.R` (a separate repo). Helplines' `helplines_aggregated_full` is loaded by `helplines_data/load_to_snowflake.R` (also separate). This repo's `models/sources.yml` just declares those tables as sources so dbt can safely reference them — it does not load them.

### Stage 2 - dbt build

`dbt build`:
1. Resolves the full dependency graph across all models
2. Runs every model in order (no model runs before its dependencies are ready)
3. Runs all configured tests, including the taxonomy-drift warning in `tests/warn_unmapped_ut1_share.sql`
4. Writes results into schemas configured per-folder in `dbt_project.yml` (see "Where things land" below)

### Stage 3 - S3 + Redis export (`loaders/export_la_queries_to_s3.R`)

Discovers every table in the `la_product` mart schema at runtime (not hardcoded), fetches each one, and writes it to:
- **S3**: `s3://asc-analytics-dashboard-backend-development-data/gloucestershire/{TABLE}.json`
- **Redis**: key `gloucestershire:{table}` (via an SSH tunnel through a bastion host)

This is Gloucestershire-only by construction — it exports whatever tables land in the `la_product` mart schema, which today is only the Gloucestershire PoC marts.

### Stage 4 - dbt docs generate

Regenerates the interactive lineage/docs site. A failure here does not fail the pipeline (docs may go briefly stale, that's all).

### Stage 5 - Observability (non-fatal)

- `dbt source freshness` — data-level check: are AVA/HELPLINES/CASEWORK source tables current?
- `snowflake_staleness_check.R` — ETL-level check via `INFORMATION_SCHEMA.LAST_ALTERED`: did the pipeline actually run?

---

## Where things land (schemas)

dbt writes into different schemas depending on which folder a model lives in, configured in `dbt_project.yml`. The Snowflake database is `ANALYTICS`; dbt prefixes custom schema names with the target schema (`PUBLIC`), so `+schema: la_product` physically resolves to `ANALYTICS.PUBLIC_LA_PRODUCT`.

| Folder | Config schema | Physical schema | Contains |
|---|---|---|---|
| `models/staging/la_product/` | `la_product_staging` | `ANALYTICS.PUBLIC_LA_PRODUCT_STAGING` | `stg_accessava`, `stg_advicepro`, `stg_helplines`, `stg_la_topic_mentions`, `stg_la_topic_mentions_glos` |
| `models/intermediate/la_product/` | `la_product_staging` (same as above — organisational split only) | `ANALYTICS.PUBLIC_LA_PRODUCT_STAGING` | `int_glos_*` (7 models, all 5 time windows pre-computed) |
| `models/staging/acs_helplines/` | `staging_acs_helplines` | `ANALYTICS.PUBLIC_STAGING_ACS_HELPLINES` | `helplines_advicepro_accessava` |
| `models/marts/la_product/` | `la_product` | `ANALYTICS.PUBLIC_LA_PRODUCT` | All 35 `mart_glos_*` models + `mart_la_query_summary` — RBAC-restricted, exported to S3/Redis |
| `models/marts/chatbot/` | (default) | `ANALYTICS.PUBLIC` | Chatbot tenant marts |

---

## Data journey: three sources, one taxonomy

```
AdvicePro API ──> CASEWORK.ADVICEPRO_DEMOGRAPHICS ──┐
                                                      │
   (postcode lookup, findthatpostcode.uk)            │
   CASEWORK.CASEWORK_LOCALITY ─────────────────────┐ │
                                                    │ │
CASEWORK.ADVICEPRO_CASEWORK ───────────────────────┤ │
                                                    ▼ ▼
                                          stg_advicepro.sql
                                  (case_topic_bridge -> s_c_csi_map ->
                                   universal_themes_map -> UT1/UT2;
                                   joins casework_locality for county)
                                                    │
AccessAva (chatbot_data repo, separate loader)      │
ACCESSAVA.ACCESSAVA + ACCESSAVA.ACCESSAVA_LOCALITY  │
                                                    ▼
                                          stg_accessava.sql
                                  (topic_entry_point -> topic_entry_point_map
                                   -> UT1/UT2; joins accessava_locality for county)
                                                    │
Helplines (helplines_data repo, separate loader)    │
HELPLINES.HELPLINES_AGGREGATED_FULL                 │
                                                    ▼
                                          stg_helplines.sql
                                  (already UT1-native, pre-aggregated by ETL;
                                   UT2/locality/age/letter all NULL)
                                                    │
                    ┌───────────────────────────────┴──────────────────────────┐
                    ▼                                                          │
          stg_la_topic_mentions.sql  (UNION ALL of all three, one row          │
          per topic mention — not one row per conversation/case)              │
                    │                                                          │
        ┌───────────┼──────────────────────────────┐                          │
        ▼           ▼                              ▼                          │
stg_la_topic_    mart_la_query_summary   helplines_advicepro_accessava        │
mentions_glos    (all LAs, all-time,     (monthly UT1/UT2 grain,              │
(Gloucestershire  LA x source x segment) staging_acs_helplines schema)        │
  filter)                                                                     │
        │                                                                     │
        ▼                                                                     │
int_glos_* (7 models, models/intermediate/la_product/, all 5 time windows)    │
        │                                                                     │
        ▼                                                                     │
mart_glos_* (35 models: 7 views x 5 windows, la_product schema, RBAC-restricted)
        │
        ▼
Stage 3 export (S3 + Redis) → LA product web views
```

---

## dbt concepts: what each component is

### Sources (`models/sources.yml`)

Sources are the raw Snowflake tables loaded by this repo's R scripts or by another repo entirely (chatbot_data, helplines_data). They are not created or modified by dbt - dbt just needs to know they exist so it can reference them safely.

```yaml
# sources.yml (simplified)
sources:
  - name: casework
    database: CASEWORK
    schema: PUBLIC
    tables:
      - name: advicepro_casework
      - name: advicepro_demographics
      - name: casework_locality
```

In a model SQL file you reference a source like this:
```sql
SELECT * FROM {{ source('casework', 'advicepro_casework') }}
```

dbt will validate the source exists before running. If the loader failed and the table is empty, tests catch it.

### Staging models (`models/staging/la_product/`)

Staging models are the first layer of transformation. Each one represents one source, mapped onto the shared UT1/UT2 Universal Theme taxonomy so all three sources become comparable.

**`stg_advicepro.sql`** - joins case, demographics, and locality, then maps to UT1/UT2:
```
ADVICEPRO_CASEWORK        (the case: date, postcode, case reference)
     +
ADVICEPRO_DEMOGRAPHICS    (the person: age band, etc.)
     +
CASEWORK_LOCALITY         (county, resolved from postcode by the loader)
     +
CASE_TOPIC_BRIDGE -> S_C_CSI_MAP -> UNIVERSAL_THEMES_MAP  (maps to UT1/UT2)
─────────────────────────────────────────────────────────────
→ one row per case x topic, with: LA_NAME, QUERY_DATE, SEGMENT (UT1), UT2,
  AGE_BAND, HAS_LETTER (always 0), LOCALITY_NAME (county)
```

**`stg_accessava.sql`** - same shape, different mapping chain (`topic_entry_point` → `topic_entry_point_map` → UT1/UT2), joins `accessava_locality` for county.

**`stg_helplines.sql`** - already UT1-native (helplines_data ETL pre-aggregates to UT1); UT2, age, locality, and letter columns are set NULL for union compatibility.

**`stg_la_topic_mentions.sql`** - `UNION ALL` of all three:
```
stg_advicepro   UNION ALL   stg_accessava   UNION ALL   stg_helplines
────────────────────────────────────────────────────────────────────
→ one row per topic mention (NOT one row per conversation/case/query —
  an interaction touching 3 topics contributes 3 rows)
   Columns: LA_NAME, QUERY_DATE, SOURCE_SYSTEM, QUERY_COUNT,
            SEGMENT (UT1), UT2, AGE_BAND, HAS_LETTER, LOCALITY_NAME
```

This is the key normalisation step. Everything downstream reads from `stg_la_topic_mentions` (or its Gloucestershire-filtered sibling `stg_la_topic_mentions_glos`) and is therefore source-agnostic. `helplines_advicepro_accessava` (a monthly UT1/UT2 aggregate, separate schema) also reads from here rather than re-implementing the mapping chain against raw sources.

### Intermediate models (`models/intermediate/la_product/`)

Between staging and marts sits a layer of Gloucestershire-specific aggregations, one per analytical angle, each pre-computing all 5 time windows (1m/3m/6m/9m/12m) into a single unsuppressed table:

```
int_glos_la_activity_summary     - total queries per LA
int_glos_la_queries_over_time    - monthly time series per LA
int_glos_la_query_source         - breakdown by source system
int_glos_la_query_segments       - breakdown by UT1 segment
int_glos_la_locality_overview    - breakdown by county
int_glos_la_demographics         - breakdown by age band
int_glos_la_legal_letters        - queries vs legal letters generated (AccessAva only)
```

These are raw, unsuppressed counts for BI use. The `mart_glos_*` models slice each one by time window and apply small-number suppression on top.

### Mart models (`models/marts/`)

Marts are the final output tables - what Power BI, S3/Redis, and web products actually query.

**Chatbot marts** (`models/marts/chatbot/`): simple conversation counts by tenant, monthly and all-time. No suppression - internal operational metrics.

**LA product marts** (`models/marts/la_product/`): 35 tables (7 view families x 5 time windows) plus `mart_la_query_summary` (all LAs, all-time, no suppression). Each `mart_glos_*` view family slices its `int_glos_*` source by `TIME_WINDOW_MONTHS` and applies `la_suppress()`.

The mart model files themselves are very short:
```sql
-- mart_glos_la_activity_summary_1m.sql
SELECT LA_NAME, QUERY_COUNT_DISPLAY, TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_activity_summary') }}
WHERE TIME_WINDOW_MONTHS = 1
```

### Macros (`macros/la_product/`)

Macros are reusable SQL fragments the `int_glos_*` models call once per time window. In Jinja (the templating language dbt uses):

```sql
-- macros/la_product/la_activity_summary.sql
{% macro la_activity_summary(months_back, source_model='stg_la_topic_mentions_glos', suppress=true) %}
SELECT
    LA_NAME,
    {% if suppress %}
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {% else %}
    SUM(QUERY_COUNT) AS QUERY_COUNT_RAW,
    {% endif %}
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1
{% endmacro %}
```

`int_glos_la_activity_summary.sql` calls this macro 5 times (once per time window) with `suppress=false`, then `UNION ALL`s the results. `mart_glos_la_activity_summary_{1m,3m,...}.sql` then slices the unsuppressed `int_glos_la_activity_summary` by `TIME_WINDOW_MONTHS` and applies `la_suppress()`.

To see what dbt will actually execute before running it:
```bash
dbt compile --select int_glos_la_activity_summary
```

### The `la_suppress()` macro (SDC/SNS suppression)

```sql
{% macro la_suppress(expr) %}
CASE WHEN {{ expr }} < 5 THEN '1-5' ELSE CAST({{ expr }} AS VARCHAR) END
{% endmacro %}
```

Any count below 5 is replaced with the string `'1-5'`. This is a row-level operation, safe to apply per time-window chunk before `UNION ALL`-ing them together. The `<5` suppression is a standard NHS/ONS SNS requirement for reporting population health data.

Note that the output column is a `VARCHAR`, not a number. Power BI and web products must handle `'1-5'` as a string. Never cast it back to a number.

---

## How dbt resolves run order

When you run `dbt build`, dbt reads all the `ref()` and `source()` calls in every model and builds a Directed Acyclic Graph (DAG):

```
source('casework', 'advicepro_casework')     ──┐
source('casework', 'advicepro_demographics') ──┤── stg_advicepro ──┐
source('casework', 'casework_locality')      ──┘                   │
                                                                     │
source('accessava', 'accessava')             ──┐                   │
source('accessava', 'accessava_locality')    ──┘── stg_accessava ──┼── stg_la_topic_mentions ──┬── stg_la_topic_mentions_glos ── int_glos_* (×7) ── mart_glos_* (×35)
                                                                     │                            ├── mart_la_query_summary
source('helplines', 'helplines_aggregated_full') ── stg_helplines ──┘                            └── helplines_advicepro_accessava
```

dbt guarantees each upstream model finishes before anything reading from it starts. You never specify this order yourself - you just declare the dependencies with `ref()` and dbt handles the rest.

---

## How to check it's working

### Command Centre dashboard

`data.accesscharity.org.uk/cc.html` - the first place to check. Shows whether the last run succeeded, how long each stage took, and links to any GitHub issues opened automatically on failure.

### dbt logs

On the VM (if you have SSH access):
```bash
tail -100 /srv/projects/dbt-asc/logs/dbt_run.log
tail -50  /srv/projects/dbt-asc/logs/load_casework_locality_to_snowflake.log
tail -50  /srv/projects/dbt-asc/logs/export_la_queries_to_s3.log
```

### dbt docs

A live documentation site is served at `data.accesscharity.org.uk/dbt-docs/` - the full model lineage graph, column descriptions, test results, and compiled SQL for every model. Fastest way to understand what any model does without reading code.

### Manual run

```bash
cd /srv/projects/dbt-asc
dbt build --select +mart_glos_la_activity_summary_1m   # + means "include all upstream models too"
dbt build --select stg_la_topic_mentions+               # trailing + means "include all downstream models too"
dbt build                                                # everything
dbt compile                                              # validate SQL parses, no Snowflake queries
```

### Checking Snowflake directly

```sql
-- Row counts across all Gloucestershire mart time windows
SELECT TIME_WINDOW_MONTHS, COUNT(*) AS row_count
FROM ANALYTICS.PUBLIC_LA_PRODUCT.MART_GLOS_LA_ACTIVITY_SUMMARY_12M
GROUP BY 1
ORDER BY 1;

-- Is stg_la_topic_mentions populated from all three sources?
SELECT SOURCE_SYSTEM, COUNT(*) AS rows, MAX(QUERY_DATE) AS latest_date
FROM ANALYTICS.PUBLIC_LA_PRODUCT_STAGING.STG_LA_TOPIC_MENTIONS
GROUP BY 1;
```

---

## How to make changes

### Adding a new mart model

1. Decide which `int_glos_*` model it reads from (or create a new one reading from `stg_la_topic_mentions_glos`)
2. Create `models/marts/la_product/mart_glos_{name}_{window}.sql` — one file per time window, or follow the existing pattern of slicing a pre-computed `int_glos_*` table
3. If the logic is reusable across time windows, add a macro in `macros/la_product/{name}.sql`
4. Run `dbt build --select mart_glos_{name}_{window}` to test it locally
5. Add column-level tests/docs to `models/marts/la_product/schema.yml`

### Changing the staging model

If you add a column to `stg_la_topic_mentions` (e.g. a new dimension from a new source), every downstream model automatically has access to it via `SELECT *`. You don't need to re-declare anything — but check `stg_helplines` and any other unioned source also emits that column (as `NULL` if not applicable), since `UNION ALL` requires matching columns across all three.

### Adding a new loader

1. Create `loaders/load_{name}_to_snowflake.R`
2. Add it to `run_pipeline.sh` Stage 1 - source system loads go first, derived loads (that depend on other loaders) go after
3. Add the target table to `models/sources.yml` so dbt can reference it
4. Document the API column mapping in `loaders/report_schemas.yml`

### Changing a macro

The macro change propagates to all models that call it on the next `dbt build`. No other files need changing.

```bash
dbt compile --select int_glos_la_activity_summary
cat target/compiled/asc/models/intermediate/la_product/int_glos_la_activity_summary.sql
```

---

## Roles and permissions

Three Snowflake roles are in play:

| Role | What it can do | Used by |
|---|---|---|
| `ROLE_ETL_WRITE` | Write to raw databases (CASEWORK, REFERENCE, ACCESSAVA) | R loaders |
| `ROLE_DBT_TRANSFORM` | Read raw databases, write ANALYTICS | dbt (Stage 2 of `run_pipeline.sh`), also default role for ETL_USER |
| `ROLE_PBI_READ` | Read-only on ANALYTICS | Power BI, web products |

The separation is intentional. The R loaders write raw data with `ROLE_ETL_WRITE`. dbt reads that raw data and writes the final ANALYTICS tables with `ROLE_DBT_TRANSFORM`. Nothing that reads from ANALYTICS can accidentally write to a raw table.

One subtlety: `ROLE_DBT_TRANSFORM` is the default role for the ETL_USER account. When the R loaders connect with `role = NULL`, they get `ROLE_DBT_TRANSFORM`. This is why the demographics loader (which does a full replace using TRUNCATE+INSERT) works - it owns the table it writes to, so it can TRUNCATE it. The locality loader uses `ROLE_ETL_WRITE` explicitly for its initial writes to `CASEWORK_LOCALITY`.

---

## Glossary

| Term | What it means here |
|---|---|
| **dbt build** | Single command that runs all models + all tests in dependency order |
| **dbt compile** | Validates SQL and expands macros without running anything in Snowflake |
| **dbt deps** | Installs packages listed in `packages.yml` (run manually when packages.yml changes) |
| **materialisation** | How dbt stores a model's output - `table` (replace entire table) or `view` (SQL view, no data stored). Every model in this project is materialised as a table (`+materialized: table` at the project root). |
| **ref()** | Jinja function that declares a dependency on another dbt model - `{{ ref('stg_la_topic_mentions') }}` |
| **source()** | Jinja function that references a raw Snowflake table managed outside dbt - `{{ source('casework', 'advicepro_casework') }}` |
| **macro** | Jinja function that generates SQL - think of it as a SQL template parameterised by dbt |
| **DAG** | Directed Acyclic Graph - the dependency tree dbt builds from all `ref()` and `source()` calls |
| **SDC/SNS** | Statistical Disclosure Control / Statistical Needs Suppression - the rule that counts below 5 must not be published |
| **grain** | The unit of analysis for a model - what one row represents. `stg_la_topic_mentions`: one topic mention (not one interaction — an interaction touching 3 topics contributes 3 rows). `mart_glos_la_activity_summary_1m`: one LA, for a fixed 1-month window. |
| **UT1 / UT2** | Universal Theme levels 1 and 2 - the shared taxonomy all three sources are mapped onto, so cross-source comparison is possible. 'Unmapped' = source value has no row in its map (taxonomy drift). 'Unmatched' = AccessAva recorded no topic at all. |
| **CASEWORK_LOCALITY** | The output of the postcode lookup - maps AdvicePro case references to county, ward, and other geography |
| **TIME_WINDOW_MONTHS** | Column in every `int_glos_*` and `mart_glos_*` table - value 1, 3, 6, 9, or 12. Filter on this to get a specific time window in Power BI |
