# External data staging

Typed, deduped views over the append-only landing tables in `EXTERNAL_DATA.LANDING`. This folder exists because the landing tables are not queryable on their own - they hold every vintage of every LA-month, so a naive `SELECT` or aggregate over them double-counts. Each model here resolves that down to one current row per LA-month before anyone else touches it.

## Models

| Model | Source table | Measure | Grain | Never |
|---|---|---|---|---|
| `stg_cld_long_term_support` | `EXTERNAL_DATA.LANDING.CLD_LONG_TERM_SUPPORT` | Stock (month-end snapshot) | `la_code, support_setting, age_group, period_start` | Sum across months |
| `stg_cld_assessments` | `EXTERNAL_DATA.LANDING.CLD_ASSESSMENTS` | Flow (monthly count) | `la_code, age_group, period_start` | Include a `Total` row in a sum (excluded here, but re-verify if the source format ever changes) |

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
   The loader targets `EXTERNAL_DATA.LANDING` - a dedicated database, not a schema tucked inside `REFERENCE` (which is curated denominator/reference data and shouldn't mix with raw landed source rows). Run `loaders/sql/create_external_data_database.sql` first if `EXTERNAL_DATA` doesn't exist yet - that's one-time DDL for whoever holds `SYSADMIN`, not something this loader or any agent session does itself. Then confirm the connecting role has `CREATE TABLE` on `EXTERNAL_DATA.LANDING`.

2. **Dry-run the loader against one dataset.** Run `load_external_sources_to_snowflake.R` with `external_source_freshness_checker`'s manifest pointed at just `cld_long_term_support` (or temporarily trim `LANDING_DATASETS` to one entry). Confirm:
   - the table is created in `EXTERNAL_DATA.LANDING`, not `REFERENCE.PUBLIC`
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

## Setting up EXTERNAL_DATA

`EXTERNAL_DATA` (database, with `LANDING`/`RAW`/`NORMALISED`/`SIGNALS` schemas) needs to exist before any of this can run - see `loaders/sql/create_external_data_database.sql`, one-time DDL for whoever holds `SYSADMIN`. Only `LANDING` is used today; `RAW` is where these staging models materialize once dbt is pointed at a real connection, `NORMALISED`/`SIGNALS` are for later work (see `_ASC/signal_processing/README.md`, "External Data Pipeline", for the full picture).

A publication_date note worth knowing before backfilling any further history: it follows `external_source_freshness_checker`'s own convention (the advertised release period, e.g. `"2026-03"`), not a page's "Last updated" revision date - see the comment block in `scripts/backfill_cld_history.py` in that repo. Two CLD releases can share a revision date without sharing a release period, and only the release period is safe to rank vintages by.
