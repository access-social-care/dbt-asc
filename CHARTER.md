# dbt-asc Charter

> Operational docs (setup, loaders, models, cron entry): [README.md](README.md)

## What this is

dbt transformation layer for Access Social Care's Snowflake data warehouse. Takes raw
data from three upstream databases (AVA, CASEWORK, HELPLINES) and produces
`ANALYTICS.PUBLIC` - the single schema consumed by Power BI and web products. Also
contains the R loaders that populate `CASEWORK.PUBLIC` from AdvicePro and Monday.com.

---

## Schema invariants

Three raw databases feed dbt:

| Database | Loaded by | Schedule |
|----------|-----------|----------|
| `AVA.PUBLIC` | `chatbot_data` repo | Daily ~05:00 |
| `CASEWORK.PUBLIC` | `loaders/load_primary_data.sh` (this repo) | Daily 06:00 |
| `HELPLINES.PUBLIC` | `helplines_data` repo | Monthly, manual |

**Web products and Power BI connect to `ANALYTICS.PUBLIC` only.** They should never
query `AVA`, `CASEWORK`, or `HELPLINES` directly. If a downstream tool appears to be
missing data, check `ANALYTICS.PUBLIC` first before touching raw schemas.

---

## `la_suppress()` changes output column type

`la_suppress(expr)` converts any count below 5 to the string `'1-5'`. **Output columns
using `la_suppress` are VARCHAR, not numeric.** Power BI must treat these columns as
text (not integers). Do not cast them to INT in Power BI transformations - a cell
containing `'1-5'` will become an error.

---

## `ROLE_PBI_READ` seeing raw, unsuppressed data is intentional

`ROLE_PBI_READ` has blanket `SELECT` on `ANALYTICS.PUBLIC` (see `setup/snowflake_permissions.sql`),
which includes unsuppressed counts. **This is by design, not a gap.** Power BI is an
internal-only tool - staff are allowed to see raw numbers. `la_suppress()` exists to protect
the Gloucestershire LA product's *external*-facing output, not anything Power BI touches.
Do not read PBI_READ's raw-data access as a disclosure-control failure.

**Separate, unresolved question:** the Gloucestershire product's own schemas
(`ANALYTICS.PUBLIC_LA_PRODUCT` for `mart_glos_*`, `ANALYTICS.PUBLIC_LA_PRODUCT_STAGING` for
`int_glos_*` - physical names per dbt's default `generate_schema_name`, no override macro exists
in this repo) have **no `GRANT` statements for either schema anywhere in this repo** -
`setup/snowflake_permissions.sql` only grants `ROLE_PBI_READ` and `ROLE_DBT_TRANSFORM` access to
`ANALYTICS.PUBLIC`, `AVA.PUBLIC`, `CASEWORK.PUBLIC`, and `REFERENCE.PUBLIC`. Since Snowflake
schema grants don't cascade from one schema to a sibling, this means who can actually read
`PUBLIC_LA_PRODUCT`/`PUBLIC_LA_PRODUCT_STAGING` - and specifically whether anything reaches the
unsuppressed `int_glos_*` layer - is not answerable from this repo. Either the grants exist,
applied directly in Snowflake and never captured here, or they don't exist yet. Worth confirming
directly in Snowflake (`SHOW GRANTS ON SCHEMA ANALYTICS.PUBLIC_LA_PRODUCT_STAGING`) rather than
assuming either way.

---

## Loader phase ordering

`loaders/load_primary_data.sh` runs in two phases:

1. Phase 1: source system loads (no dependencies) - AdvicePro demographics, Monday.com
2. Phase 2: derived loads (must run after phase 1) - postcode→LA lookup

Phase 2 reads postcode data written by Phase 1. Running Phase 2 alone produces empty
or stale locality joins. The orchestration in `run_pipeline.sh` enforces this order -
do not run phases out of order.

---

## Postcode lookup invariant

AdvicePro stores cases with client postcodes, not LA names. The locality loader
(Phase 2) resolves postcodes to LA names via `findthatpostcode.uk`. AccessAva already
knows the deploying tenant's LA - no postcode lookup is needed for chatbot data.
Do not add a postcode lookup step for chatbot records.

---

## `stg_la_queries` is the integration point

`stg_la_queries.sql` is the single UNION ALL that combines AdvicePro cases and
AccessAva conversations into one row shape. All 7 LA product mart models read from
this staging model, not directly from raw tables. Changes to mart model behaviour
start here.

---

## `mart_chatbot_*` vs LA product marts

Two `mart_chatbot_*` models exist for internal chatbot team use (conversation counts
by tenant, monthly and all-time). These are separate from the 7 LA product models -
different consumers, different grain. Do not conflate them.

---

## Package updates require manual `dbt deps`

If `packages.yml` changes (new dbt package added), run `dbt deps` manually on the VM
before the next cron run. It is not part of the daily pipeline. The cron will fail
with a missing package error if this step is skipped.

---

## Audit triggers

Re-read and update this charter when:
- A new raw database or schema is added as a dbt source
- `la_suppress` threshold changes (currently 5)
- A new mart model is added
- The pipeline cron schedule changes
