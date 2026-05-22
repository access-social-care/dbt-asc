# dbt-asc Pipeline: How it all works

A technical explainer for anyone approaching this repo without prior dbt experience.

---

## The one-sentence version

Raw data from three external systems lands in Snowflake. dbt reads it, cleans it, joins it, and writes a set of final tables that Power BI and web products consume. This repo contains both the code that loads the raw data (R scripts) and the code that transforms it (dbt SQL).

---

## Why dbt?

Before dbt, "transforming data in a warehouse" usually meant either:
- Writing SQL views that reference each other with no enforced order
- Writing a big script that runs `CREATE TABLE AS SELECT ...` statements in a carefully maintained sequence

Both approaches break down as soon as you have more than a handful of tables. You lose track of which table depends on what, tests live in spreadsheets nobody updates, and every column rename requires a grep across dozens of files.

dbt solves this with a few core ideas:
1. **Each `.sql` file is one table** — you write a `SELECT` statement, dbt runs it and materialises the result
2. **You declare dependencies with `ref()`** — instead of hardcoding `CASEWORK.PUBLIC.ADVICEPRO_CASEWORK`, you write `{{ ref('stg_advicepro') }}` and dbt figures out the order to run everything
3. **Tests are part of the project** — you declare `not_null`, `unique`, `accepted_values` tests in YAML; dbt runs them after every build
4. **One command does everything** — `dbt build` builds every table in the right order and runs every test

---

## The two-stage daily pipeline

The pipeline runs every morning in two stages, 45 minutes apart:

```
06:00  load_primary_data.sh   →  Snowflake (raw tables)
06:45  run_dbt.sh             →  Snowflake (ANALYTICS schema)
```

These are deliberately separate. If a dbt model fails, you can re-run `run_dbt.sh` without re-pulling all the API data. If an API loader fails, dbt will fail gracefully on stale data rather than silently producing wrong numbers.

### Stage 1 — Loaders (`load_primary_data.sh`)

Four R scripts run in dependency order:

| Script | What it does | Why the order matters |
|---|---|---|
| `load_member_orgs_to_snowflake.R` | Pulls Access Social Care member organisations from Monday.com → `REFERENCE.MEMBER_ORGANISATIONS` | No dependencies |
| `load_advicepro_demographics_to_snowflake.R` | Pulls all AdvicePro casework records from the API → `CASEWORK.ADVICEPRO_DEMOGRAPHICS` | No dependencies |
| `load_casework_locality_to_snowflake.R` | Reads case postcodes written by the demographics loader, looks each one up via [findthatpostcode.uk](https://findthatpostcode.uk), appends new LA names to `CASEWORK.CASEWORK_LOCALITY` | **Must run after demographics** — it reads postcodes from the table written in step 2 |

**Why the postcode lookup?** AdvicePro stores the client's postcode, not their local authority. There is no LA field in the raw AdvicePro API response. The locality loader is the bridge. It works incrementally — it only looks up postcodes for cases not already in `CASEWORK_LOCALITY`, so it doesn't hammer the external API every day.

AccessAva (the chatbot) is different. It already knows which LA a user belongs to because that's set when the LA signs up. So no postcode lookup is needed for that source — the LA name comes through directly.

### Stage 2 — dbt (`run_dbt.sh`)

`run_dbt.sh` does exactly one thing: runs `dbt build`. That single command:
1. Resolves the full dependency graph across all models
2. Runs every model in order (no model runs before its dependencies are ready)
3. Runs all configured tests
4. Writes results to `ANALYTICS.PUBLIC` in Snowflake

---

## Data journey: from API call to Power BI

Here is the full path a single piece of AdvicePro casework data takes:

```
AdvicePro API
    │
    │  (load_advicepro_demographics_to_snowflake.R, daily 06:00)
    ▼
CASEWORK.PUBLIC.ADVICEPRO_DEMOGRAPHICS
    │
    │  (load_casework_locality_to_snowflake.R, daily 06:00, reads postcodes, writes LA names)
    ▼
CASEWORK.PUBLIC.CASEWORK_LOCALITY  ─────────────────────────────┐
                                                                  │
CASEWORK.PUBLIC.ADVICEPRO_CASEWORK ──────────────────────────────┤
                                                                  │
CASEWORK.PUBLIC.ADVICEPRO_DEMOGRAPHICS ──────────────────────────┤
    │                                                             │
    │  (dbt: models/staging/la_product/stg_advicepro.sql)        │
    │  3-way JOIN: case + demographics + locality                 │
    ▼                                                             │
ANALYTICS.PUBLIC.STG_ADVICEPRO                                   │
    │                                                             │
    │  (dbt: models/staging/la_product/stg_la_queries.sql)       │
    │  UNION ALL with AccessAva data                              │
    ▼
ANALYTICS.PUBLIC.STG_LA_QUERIES
    │
    │  (dbt: models/marts/la_product/mart_la_*.sql via macros)
    │  7 mart models, each with 5 time windows as rows
    ▼
ANALYTICS.PUBLIC.MART_LA_ACTIVITY_SUMMARY
ANALYTICS.PUBLIC.MART_LA_DEMOGRAPHICS
ANALYTICS.PUBLIC.MART_LA_LEGAL_LETTERS
... (7 total)
    │
    │  (Power BI / web product connects to ANALYTICS.PUBLIC only)
    ▼
Power BI dashboard / LA product web views
```

The AccessAva path is simpler (no postcode lookup needed):
```
AccessAva chatbot system
    │
    │  (chatbot_data repo: data_uploader.R, daily ~05:00)
    ▼
AVA.PUBLIC.ACCESSAVA
    │
    │  (dbt: stg_la_queries.sql UNION ALL with stg_advicepro)
    ▼
ANALYTICS.PUBLIC.STG_LA_QUERIES
    │  (same mart models as above)
```

---

## dbt concepts: what each component is

### Sources (`models/sources.yml`)

Sources are the raw Snowflake tables that the loaders write to. They are not created or modified by dbt — dbt just needs to know they exist so it can reference them safely.

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

### Staging models (`models/staging/`)

Staging models are the first layer of transformation. Each one represents one logical dataset — it cleans, joins, and standardises the raw data into a consistent shape.

**`stg_advicepro.sql`** — joins three raw tables into one row per AdvicePro case:
```
ADVICEPRO_CASEWORK       (the case: date, postcode, case reference)
     +
ADVICEPRO_DEMOGRAPHICS   (the person: age band, etc.)
     +
CASEWORK_LOCALITY        (the LA name, resolved from postcode by the loader)
─────────────────────────────────────────────────────────────
→ one row per case, with: LA_NAME, QUERY_DATE, AGE_BAND, HAS_LETTER, LOCALITY_NAME
```

**`stg_la_queries.sql`** — UNION ALL of AdvicePro and AccessAva into a single grain:
```
stg_advicepro  (AdvicePro cases, SOURCE_SYSTEM = 'AdvicePro')
    UNION ALL
AVA.PUBLIC.ACCESSAVA  (chatbot interactions, SOURCE_SYSTEM = 'AccessAva')
────────────────────────────────────────────────────────────
→ one row per "query event", regardless of source
   Columns: LA_NAME, QUERY_DATE, SOURCE_SYSTEM, QUERY_COUNT,
            SEGMENT, AGE_BAND, HAS_LETTER, LOCALITY_NAME
```

This is the key normalisation step. Every downstream mart model reads from `stg_la_queries` and is therefore source-agnostic. Adding a third data source (e.g. helplines) means updating only this one model.

### Mart models (`models/marts/`)

Marts are the final output tables — what Power BI and web products actually query. They are aggregated, business-ready, and apply SDC suppression rules.

There are two groups:

**Chatbot marts** (`models/marts/chatbot/`): simple conversation counts by tenant, monthly and all-time. No suppression — these are internal operational metrics.

**LA product marts** (`models/marts/la_product/`): seven tables, one per analytical view. Each table contains all five time windows (1m, 3m, 6m, 9m, 12m) as rows, distinguished by a `TIME_WINDOW_MONTHS` column.

```
mart_la_activity_summary      — total queries per LA (all sources)
mart_la_queries_over_time     — monthly time series per LA
mart_la_query_source          — query breakdown by source system (AdvicePro vs AccessAva)
mart_la_query_segments        — breakdown by topic/supercategory (AccessAva only)
mart_la_locality_overview     — breakdown by locality within LA (AccessAva only)
mart_la_demographics          — breakdown by age band (AccessAva only, <5% populated)
mart_la_legal_letters         — queries vs legal letters generated (AccessAva only)
```

The mart model files themselves are very short:
```sql
-- mart_la_activity_summary.sql
{{ la_activity_summary(months_back=1) }}
UNION ALL
{{ la_activity_summary(months_back=3) }}
UNION ALL
{{ la_activity_summary(months_back=6) }}
UNION ALL
{{ la_activity_summary(months_back=9) }}
UNION ALL
{{ la_activity_summary(months_back=12) }}
```

The actual SQL logic lives in the macros (see below).

### Macros (`macros/la_product/`)

Macros are reusable SQL fragments — think of them as functions that generate SQL. In Jinja (the templating language dbt uses), a macro looks like this:

```sql
-- macros/la_product/la_activity_summary.sql
{% macro la_activity_summary(months_back) %}
SELECT
    LA_NAME,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref('stg_la_queries') }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1
{% endmacro %}
```

When dbt compiles `mart_la_activity_summary.sql`, it expands each macro call into the full SQL and then UNION ALLs them together. The final SQL that runs in Snowflake looks like this:

```sql
SELECT LA_NAME,
       CASE WHEN SUM(QUERY_COUNT) < 5 THEN '1-5'
            ELSE CAST(SUM(QUERY_COUNT) AS VARCHAR) END AS QUERY_COUNT_DISPLAY,
       1 AS TIME_WINDOW_MONTHS
FROM ANALYTICS.PUBLIC.STG_LA_QUERIES
WHERE QUERY_DATE >= DATEADD('month', -1, CURRENT_DATE())
GROUP BY 1
UNION ALL
SELECT LA_NAME,
       CASE WHEN SUM(QUERY_COUNT) < 5 THEN '1-5'
            ELSE CAST(SUM(QUERY_COUNT) AS VARCHAR) END AS QUERY_COUNT_DISPLAY,
       3 AS TIME_WINDOW_MONTHS
FROM ANALYTICS.PUBLIC.STG_LA_QUERIES
WHERE QUERY_DATE >= DATEADD('month', -3, CURRENT_DATE())
GROUP BY 1
-- ... and so on for 6, 9, 12 months
```

To see what dbt will actually execute before running it:
```bash
dbt compile --select mart_la_activity_summary
# output lands in target/compiled/dbt_asc/models/marts/la_product/mart_la_activity_summary.sql
```

### The `la_suppress()` macro (SDC/SNS suppression)

`la_suppress()` is a one-liner that applies Statistical Disclosure Control:

```sql
{% macro la_suppress(expr) %}
CASE WHEN {{ expr }} < 5 THEN '1-5' ELSE CAST({{ expr }} AS VARCHAR) END
{% endmacro %}
```

Any count below 5 is replaced with the string `'1-5'`. This is a row-level operation — there is no cross-row dependency, which is why it is safe to apply inside each time-window chunk before UNION ALL-ing them together. The `<5` suppression is a standard NHS/ONS SNS requirement for reporting population health data.

Note that the output column is a `VARCHAR`, not a number. This is intentional — Power BI and web products must handle `'1-5'` as a string. Never cast it back to a number.

---

## How dbt resolves run order

When you run `dbt build`, dbt reads all the `ref()` and `source()` calls in every model and builds a Directed Acyclic Graph (DAG):

```
source('casework', 'advicepro_casework')  ──┐
source('casework', 'advicepro_demographics') ─┤── stg_advicepro ──┐
source('casework', 'casework_locality')    ──┘                     │
                                                                    ├── stg_la_queries ── mart_la_* (×7)
source('ava', 'accessava')  ────────────────────────────────────────┘
```

dbt guarantees that `stg_advicepro` is fully built before `stg_la_queries` starts, and that `stg_la_queries` is fully built before any mart model starts. You never specify this order yourself — you just declare the dependencies with `ref()` and dbt handles the rest.

---

## How to check it's working

### Command Centre dashboard

`data.accesscharity.org.uk/cc.html` — the first place to check.

Shows:
- Whether the last run succeeded or failed
- How long each stage took
- Links to any GitHub issues opened automatically on failure

### dbt logs

On the VM (if you have SSH access):
```bash
tail -100 /srv/projects/dbt-asc/logs/dbt_run.log   # most recent dbt output
tail -50  /srv/projects/dbt-asc/loaders/load_casework_locality_to_snowflake.log  # per-loader logs
```

Loader logs are written per-script by `load_primary_data.sh` and overwritten each run.

### dbt docs

A live documentation site is served at `data.accesscharity.org.uk/dbt-docs/`. It shows:
- The full model lineage graph (interactive — you can click on any node to see its SQL, tests, and upstream/downstream dependencies)
- All column descriptions and test results
- The compiled SQL for every model

This is the fastest way to understand what any model does without reading code.

### Manual run

To run a single model and its upstream dependencies:
```bash
cd /srv/projects/dbt-asc
dbt build --select +mart_la_activity_summary   # the + means "include all upstream models too"
dbt build --select stg_la_queries+             # the trailing + means "include all downstream models too"
```

To run everything:
```bash
dbt build
```

To only compile (no Snowflake queries, just validate the SQL parses):
```bash
dbt compile
```

### Checking Snowflake directly

All output lands in `ANALYTICS.PUBLIC`. You can verify a run worked:
```sql
-- How many rows in the last 24h?
SELECT TIME_WINDOW_MONTHS, COUNT(*) AS row_count
FROM ANALYTICS.PUBLIC.MART_LA_ACTIVITY_SUMMARY
GROUP BY 1
ORDER BY 1;

-- Is stg_la_queries populated from both sources?
SELECT SOURCE_SYSTEM, COUNT(*) AS rows, MAX(QUERY_DATE) AS latest_date
FROM ANALYTICS.PUBLIC.STG_LA_QUERIES
GROUP BY 1;
```

---

## How to make changes

### Adding a new mart model

1. Decide which staging model it reads from (probably `stg_la_queries`)
2. Create `models/marts/la_product/mart_la_{name}.sql` with a `SELECT` statement
3. If the logic is reusable across time windows, create `macros/la_product/{name}.sql` with a `{% macro %}` block
4. Run `dbt build --select mart_la_{name}` to test it locally
5. Add any column-level tests to `models/marts/la_product/schema.yml`

### Changing the staging model

If you add a column to `stg_la_queries` (e.g. a new dimension from a new source), every downstream mart model automatically has access to it. You don't need to re-declare anything — dbt propagates changes through the graph.

### Adding a new loader

1. Create `loaders/load_{name}_to_snowflake.R`
2. Add it to `load_primary_data.sh` — source system loads go in the first block, derived loads (that depend on other loaders) go in the second block
3. Add the target table to `models/sources.yml` so dbt can reference it
4. Document the API column mapping in `loaders/report_schemas.yml`

### Changing a macro

The macro change propagates to all models that call it on the next `dbt build`. No other files need changing.

If you need to preview the compiled SQL after a macro change:
```bash
dbt compile --select mart_la_activity_summary
cat target/compiled/dbt_asc/models/marts/la_product/mart_la_activity_summary.sql
```

---

## Roles and permissions

Three Snowflake roles are in play:

| Role | What it can do | Used by |
|---|---|---|
| `ROLE_ETL_WRITE` | Write to raw databases (CASEWORK, REFERENCE, AVA) | R loaders |
| `ROLE_DBT_TRANSFORM` | Read raw databases, write ANALYTICS | dbt (`run_dbt.sh`), also default role for ETL_USER |
| `ROLE_PBI_READ` | Read-only on ANALYTICS | Power BI, web products |

The separation is intentional. The R loaders write raw data with `ROLE_ETL_WRITE`. dbt reads that raw data and writes the final ANALYTICS tables with `ROLE_DBT_TRANSFORM`. Nothing that reads from ANALYTICS can accidentally write to a raw table.

One subtlety: `ROLE_DBT_TRANSFORM` is the default role for the ETL_USER account. When the R loaders connect with `role = NULL`, they get `ROLE_DBT_TRANSFORM`. This is why the demographics loader (which does a full replace using TRUNCATE+INSERT) works — it owns the table it writes to, so it can TRUNCATE it. The locality loader uses `ROLE_ETL_WRITE` explicitly for its initial writes to `CASEWORK_LOCALITY`.

---

## Glossary

| Term | What it means here |
|---|---|
| **dbt build** | Single command that runs all models + all tests in dependency order |
| **dbt compile** | Validates SQL and expands macros without running anything in Snowflake |
| **dbt deps** | Installs packages listed in `packages.yml` (run manually when packages.yml changes) |
| **materialisation** | How dbt stores a model's output — `table` (replace entire table) or `view` (SQL view, no data stored). Staging models are views; mart models are tables. |
| **ref()** | Jinja function that declares a dependency on another dbt model — `{{ ref('stg_la_queries') }}` |
| **source()** | Jinja function that references a raw Snowflake table managed outside dbt — `{{ source('casework', 'advicepro_casework') }}` |
| **macro** | Jinja function that generates SQL — think of it as a SQL template parameterised by dbt |
| **DAG** | Directed Acyclic Graph — the dependency tree dbt builds from all `ref()` and `source()` calls |
| **SDC/SNS** | Statistical Disclosure Control / Statistical Needs Suppression — the rule that counts below 5 must not be published |
| **grain** | The unit of analysis for a model — what one row represents. `stg_la_queries`: one interaction event. `mart_la_activity_summary`: one LA × one time window. |
| **CASEWORK_LOCALITY** | The output of the postcode lookup — maps AdvicePro case references to LA names |
| **TIME_WINDOW_MONTHS** | Column in every LA product mart — value 1, 3, 6, 9, or 12. Filter on this to get a specific time window in Power BI |
