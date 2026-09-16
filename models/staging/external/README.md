# External data staging

Typed, deduped views over the append-only landing tables in `REFERENCE.LANDING`. This folder exists because the landing tables are not queryable on their own - they hold every vintage of every LA-month, so a naive `SELECT` or aggregate over them double-counts. Each model here resolves that down to one current row per LA-month before anyone else touches it.

## Models

| Model | Source table | Measure | Grain | Never |
|---|---|---|---|---|
| `stg_cld_long_term_support` | `REFERENCE.LANDING.CLD_LONG_TERM_SUPPORT` | Stock (month-end snapshot) | `la_code, support_setting, age_group, period_start` | Sum across months |
| `stg_cld_assessments` | `REFERENCE.LANDING.CLD_ASSESSMENTS` | Flow (monthly count) | `la_code, age_group, period_start` | Include a `Total` row in a sum (excluded here, but re-verify if the source format ever changes) |

Full column contracts, test coverage, and the reasoning behind each transform are in `schema.yml` and the model files themselves - this README is for the shape of the system, not the field-by-field detail.

## What each model does, and why it's here rather than upstream

The landing tables carry the source file close to verbatim (see `models/sources.yml`, `data_portal_landing` source) - `external_source_freshness_checker` lands rows as-is, on purpose, so nothing about a publisher's raw layout is silently decided at extraction time. That means every model here earns its place by doing something the landing table genuinely can't:

1. **Drop `Total` rows.** The publisher's wide file has a trailing Total column per LA; landed long, it arrives as a metric row indistinguishable from a real month unless excluded by name.
2. **Parse the month header into `period_start`/`period_end`.** The header is prose (`"July 2025 [p]"`), not a date - anything joining or filtering by period needs it typed.
3. **Type `value`, and separate "suppressed" from "absent".** A `[c]` marker means the real number was withheld, not that it was zero. Left untouched, an aggregate over suppressed cells is silently biased down - worst on exactly the small segments most likely to be suppressed.
4. **Resolve to one row per LA-month.** Each quarterly release covers a rolling window, not a fixed one - a later release can both revise months the previous release also covered and drop months only the previous release had. The dedupe is **per period**, not per file: `ROW_NUMBER() OVER (PARTITION BY <grain> ORDER BY publication_date DESC)`. A month covered by only one vintage keeps it regardless of age, since nothing newer competes for that partition.

Why two models instead of one combined table: long-term-support is a stock measure (mean or latest-snapshot is the valid aggregation) and assessments is a flow (sum is valid). A shared table eventually gets summed by someone who doesn't know which column is which; keeping them apart makes that mistake impossible rather than merely documented.

## Verifying this against a live warehouse

This code was written and committed without a Snowflake connection available in that session - every model, source definition, and loader change here is unexercised against a real account. That's a plain fact about this commit's history, not a property of the design; the checklist below is what turns it into verified code, and once run through once it stays verified going forward like any other model.

Run these in order. Stop and fix before continuing if any step fails.

1. **Confirm the role can write where this expects.**
   ```sql
   SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
   ```
   The loader targets `REFERENCE.LANDING` (a schema inside the existing `REFERENCE` database, not a new database - see the `LANDING_DB`/`LANDING_SCHEMA` comment block at the top of `loaders/load_external_sources_to_snowflake.R` for why). Confirm the role has `CREATE TABLE` on `REFERENCE.LANDING`, or `CREATE SCHEMA` on `REFERENCE` if the schema doesn't exist yet.

2. **Dry-run the loader against one dataset.** Run `load_external_sources_to_snowflake.R` with `external_source_freshness_checker`'s manifest pointed at just `cld_long_term_support` (or temporarily trim `LANDING_DATASETS` to one entry). Confirm:
   - the table is created in `REFERENCE.LANDING`, not `REFERENCE.PUBLIC`
   - column names landed as unquoted snake_case (`support_setting`, not `"Support setting"`)
   - re-running the loader immediately afterward is a no-op (the vintage guard should log a skip, not double the row count)

3. **Compile and run the staging models.**
   ```bash
   dbt run --select stg_cld_long_term_support stg_cld_assessments
   dbt test --select stg_cld_long_term_support stg_cld_assessments
   ```
   Watch specifically for `assert_no_total_metric_in_cld`, `assert_cld_metric_parses`, and the `dbt_utils.unique_combination_of_columns` grain test in `schema.yml` - these three are the ones standing in for bugs that were previously live in production (the Total double-count and the period-derivation bug, both found and fixed in `signal_processing` this same week).

4. **Spot-check real numbers.** Pick one LA, sum its `stg_cld_assessments` rows for a known 12-month window, and compare against the published `Total [p]` figure in the source file for that LA (allow a few points of difference - the publisher's own disclosure-control rounding means the total and the sum of rounded months don't always match exactly, only closely).

5. **Append behaviour, once a second vintage exists.** Re-run the loader after the *next* quarterly CLD release lands. Confirm the landing table gained a new vintage's rows (didn't overwrite), and that `stg_cld_*` picked the newer value for months both vintages cover while keeping any month only the older vintage had.

## When EXTERNAL_DATA gets provisioned

The design target (see `_ASC/signal_processing/README.md`, "External Data Pipeline") is a dedicated `EXTERNAL_DATA` database with `LANDING`/`RAW`/`NORMALISED`/`SIGNALS` schemas. That didn't happen in this pass because `CREATE DATABASE` needs `SYSADMIN`, which wasn't available to verify. When it is: change `LANDING_DB`/`LANDING_SCHEMA` in the loader, update the `database:` on the `data_portal_landing` source in `models/sources.yml`, and one-off copy the existing `REFERENCE.LANDING` rows across. Nothing else in the loader or these models hardcodes `REFERENCE`.
