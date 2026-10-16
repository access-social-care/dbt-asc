# Parity summary: external data, reference data and freshness slice

Date: 2026-10-15. `raw_external.main` holds 35 tables (32 + 3 CLD). DuckDB = MotherDuck `staging` and `marts` (models) and `raw_external` / `reference` (sources). Snowflake read-only (ROLE_DEV). Per-table detail: the other files in this folder (summary only, no data rows).

## Models built (`dbt build --target duckdb --select tag:external`: 43 of 43 PASS, 0 WARN)

| model (DuckDB) | Snowflake equivalent | rows | key set | columns failing | verdict |
|---|---|---|---|---|---|
| `staging.main.stg_cld_assessments` | `EXTERNAL_DATA.RAW.STG_CLD_ASSESSMENTS` | 78390 = 78390 | match | 0 of 20 | PASS |
| `staging.main.stg_cld_long_term_support` | `EXTERNAL_DATA.RAW.STG_CLD_LONG_TERM_SUPPORT` | 391950 = 391950 | match | 0 of 20 | PASS |
| `marts.main.norm_cld_assessments` | `EXTERNAL_DATA.NORMALISED.NORM_CLD_ASSESSMENTS` | 45063 = 45063 | match | 0 of 23 | PASS |
| `marts.main.norm_cld_long_term_support` | `EXTERNAL_DATA.NORMALISED.NORM_CLD_LONG_TERM_SUPPORT` | 195733 = 195733 | match | 0 of 23 | PASS |
| `staging.main.cld_published_population` (seed) | `EXTERNAL_DATA.NORMALISED.CLD_PUBLISHED_POPULATION` | 4510 = 4510 | match | 0 of 7 | PASS |

`stg_cld_*` on DuckDB are pass-throughs over `raw_external.main.cld_*`, which already hold the stacked, typed, deduped output. The macro `cld_stacked_raw` renders the original Snowflake SQL on every other target. Parity of the pass-through is therefore parity of the loaded tables.

## Source tables: MotherDuck vs Snowflake (row count, column set, per-column aggregates, key-set hash)

| group | tables | result |
|---|---|---|
| `raw_external.main` data portal and Census/LANN | 32 compared against `REFERENCE.PUBLIC` (`ascfr_extract` against `ASCFR_SALT`) | 32 of 32 PASS, row counts equal, 0 failing columns |
| `raw_external.main.cld_*`, `cld_published_population` | 3 (see models above) | PASS |
| `reference.main` | 15 compared against `REFERENCE.PUBLIC` | 15 of 15 PASS, row counts equal, 0 failing columns |

## Coverage differences

| item | status | cause |
|---|---|---|
| `EXTERNAL_DATA.LANDING` visible to ROLE_DEV: `CLD_ASSESSMENTS` (73788), `CLD_LONG_TERM_SUPPORT` (340560) only | expected | Landing holds the age-group sheet only; stg stacks gender and ethnicity on top (78390 and 391950). The 4 gender/ethnicity landing tables, `ASC_WORKFORCE` and the 4 `monday_*` landing tables return "does not exist or not authorized" for ROLE_DEV, so they cannot be compared. MotherDuck has none of them under those names. |
| `raw_external.main.asc_workforce` (152 rows) | staleness risk | Equals the old wide `REFERENCE.PUBLIC.ASC_WORKFORCE`, which the loader no longer writes. The annual-vintage landing table is not readable by ROLE_DEV, so currency against it is unverified. |
| `discharge_delays`, `chc_fnc`, `household_carers` | missing both sides | Declared in `sources.yml`, never loaded to Snowflake or MotherDuck. Already `freshness: null`. |
| `REFERENCE.PUBLIC`: `MAPPING_PROMOTION_LOG`, `ASC_WORKFORCE_2024_25_BACKUP`, legacy `CLD_ASSESSMENTS` (1989) and `CLD_LONG_TERM_SUPPORT` (1836) | absent on MotherDuck | Operational log, backup and superseded tables. No dbt source reads them. Expected. |
| Census (10) and LANN (9) tables | mapped | Live in `raw_external.main`, not `reference.main`. Twin source `reference_external` (duckdb only) added; the `reference` entries are disabled on duckdb. |
| `ascfr_salt` | mapped | MotherDuck name is `ascfr_extract`; `identifier` set per target. |

No bugs found in this slice. One harness limitation: `parity.py` casts numeric key columns to text, and DuckDB DOUBLE renders `2021.0` where Snowflake renders `2021`, so a numeric key gives a false key-set DIFFER. Use text key columns.

## Freshness on DuckDB

`dbt source freshness --target duckdb --select source:data_portal` checks 7 sources, all PASS (cqc_ratings, people_receiving_care, dols, population, ombudsman, ascof, safeguarding_adults). Field: `_extracted_at` (the asc-agent fetch time, a naive US-Pacific clock, so up to about 8 hours early against UTC, immaterial at day-scale thresholds). Thresholds are the existing ones. Snowflake keeps metadata-based freshness (`loaded_at_field` renders to none; confirmed by manifest diff).

Not covered here: `accessava`, `accessava_locality`, `helplines_aggregated_full` still warn that the adapter has no metadata-based freshness; they belong to the chatbot and helplines slices. `data_portal_landing.cld_*` stay `freshness: null` by existing design. Tables without an extraction timestamp (Census, LANN, reference lookups) have no freshness expectation.
