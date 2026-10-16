# Helplines on MotherDuck: build and parity summary

Date 2026-10-15. Branch `feat/duckdb-helplines` (stacked on `feat/duckdb-target`). Oracle: `partner_readers.R` and
`data_aggregator.R` on helplines_data `main`; Snowflake `HELPLINES.PUBLIC.PARTNER_HISTORY_*` and
`HELPLINES_AGGREGATED_FULL` (read-only, ROLE_DEV). Per-month CSVs: `helplines_<partner>_months.csv` (CAB: not saved, see table).
Harness: `scripts/parity/helplines_parity.py` (per month: row count plus order-independent hashes of grouped rows, per column).
Verdicts: PARITY, UT_MAP_DRIFT (t1, t2, LA identical, only topic tags differ: map changed after Snowflake tagged the rows),
VALUES_DIFFER, COUNT_DIFFERS, ONLY_SNOWFLAKE (month not in raw.share_point), ONLY_DUCKDB.
UT1 spelling variants the aggregator normalises, UT2 literal `NA` and Scope's `999.0` sentinel are compared normalised.

## Inventory (raw.share_point, 145 tables, every column VARCHAR/TIMESTAMPTZ per drop)

| Partner | Tables | Layout and overlap |
|---|---|---|
| Age UK | 17 | 8 T1 + 8 T3 workbooks paired by date key (overlapping windows, one is a 2019-2026 full series) + 1 junk sheet |
| Carers UK | 30 | monthly xlsx, Civi monthly, 7 cumulative "annual report to <month>" (each repeats earlier months), 4 full_series tables (reparse branch only) |
| Citizens Advice | 2 | two Tableau exports overlapping from 2023-04 |
| Ealing | 6 | 4 old-layout files (header lands as row 0) and 2 new-layout files; Fivetran maps both onto one column set by position |
| Independent Age | 17 | rolling 12-month windows, each month in up to 12 files |
| Mencap / RNIB | 2 + 5 | compiled seed history (both orgs), FY25/26 annual files, an older RNIB FY23/24 workbook |
| Scope | 66 | 11 monthly drops x 4 identical data sheets (+dict/example sheets) |

## Per-partner status (months inside the raw coverage)

| Partner | Model | Status | Months: PARITY / drift / differ / only SF / only DuckDB | Root cause of the rest |
|---|---|---|---|---|
| Citizens Advice | partner_history_cab | PARITY | 49 / 0 / 0 / 35 / 0 (run before CSVs were kept) | only SF: 2019-04..2022-04 predate the oldest file on SharePoint |
| Mencap | partner_history_mencap | PARITY | 84 / 0 / 0 / 0 / 0 | none |
| Independent Age | partner_history_ia | PARITY | 26 / 0 / 0 / 57 / 0 | only SF: months before the oldest window. Includes R's `unique()` undercount (see decisions) |
| Carers UK | partner_history_carers_uk | PARITY | 21 / 0 / 0 / 66 / 0 | only SF: 2019..2024-09 came from files not on SharePoint; 2026-07 not yet landed |
| Ealing | partner_history_ealing | PARITY (rows) | 22 / 7 / 0 / 59 / 0 | drift: UT2 labels added to the map since (2 months also UT1) |
| RNIB | partner_history_rnib | DIFFERS | 54 / 0 / 1 / 0 / 11 | the FY23/24 workbook on SharePoint was never loaded to Snowflake (12 months, Jan 2024 would be replaced by the fuller version) |
| Scope | partner_history_scope | DIFFERS | 0 / 0 / 21 / 40 / 0 | LA name and code match in every month. 14 months: same counts, but T1 text differs (trailing spaces such as `direct payments `, relabelled topics) so rows fail the map. 7 months (2025-05..12): Snowflake holds an earlier export with different labels and counts; the SharePoint sheets are later re-exports |
| Age UK | partner_history_age_uk | DIFFERS | 0 / 0 / 87 / 0 / 7 | Snowflake holds first-read snapshots; the SharePoint pairs are later re-exports (trailing spaces in T2, more LA filled, a few cases added or removed). ONLY_DUCKDB: 2019-01..03 and 2026-07..10, outside Snowflake history. Unrecoverable from raw |

Mart (`marts.main.helplines_ongoing`, `staging.main.helplines_stable_period`): built (263 rows, 2024-04..2026-03, primary stable period 24 months,
5.6 partners average). Month totals sit within 1-10% of Snowflake for 2024-04..2026-03 and differ where Age UK and Scope differ; Snowflake's mart
spans 2020-01..2026-05 because its history is fuller. `scripts/parity/helplines_mart_check.py` (dbt SQL on Snowflake's own history vs the live
mart) is written but NOT RUN: the MotherDuck daily compute limit was reached. Mart logic is therefore unverified in isolation.

## Tests (all 70 selected by tag:helplines passed before the limit stop)

Canonical shape (nine columns, DATE then VARCHAR) per model, row-count floors, `org` constant, UT1 domain (KNOWN_GOOD_UT1), GSS code shape, mart not-null and n > 0,
stable period present. These replace the promote scripts' shape check and row-count guard.

## Decisions for the helplines_data CHARTER

- One source per month is chosen by "fullest wins, ties to the earlier table name" (Mencap/RNIB's rule, now for all but Age UK, which uses "earliest-ending pair"). "First-seen" cannot be reproduced from Fivetran tables (no load order); this reproduces Snowflake for CAB and Ealing.
- Independent Age: R's `unique()` in `parse_ia()` collapses cases sharing topic, county and day. Reproduced for parity; it undercounts (case numbers are reused). Decide whether to keep it (reparse branch removes it).
- Ealing old layout: R drops the first data row (skip = 1 then removes row id 1). Reproduced; it loses one real record per file. Likely a bug.
- Scope and Age UK exports now carry trailing spaces in topic labels; R does not trim, so those rows are unmapped (UT1 NULL) and silently leave the mart. Decide whether to trim before the map join.
- Scope: R reads the last sheet only; the dbt layer reads every data sheet. Re-exports relabel topics between drops, so a month's label set depends on the drop chosen.
- LA reference data (la_25, all_geo_to_la_map, la_to_cassr from ascFuncs) are seeds in `seeds/helplines/`; they belong in REFERENCE. `reference.la_name_map` is already there and equals `ascFuncs::la_map`; `reference.universal_themes_map` equals main's `Universal codes.csv`. Reference table names are `universal_themes_map`, not `mapping_*`.
- Stable period uses the code's threshold of 5 partners (charter says 6, recorded conflict).
- Snowflake history carries map-vintage drift (UT2 `NA`, old UT1 spellings, retired placeholder). Not retagged, as per the charter; the dbt layer re-tags on every build, i.e. changes history when the map changes.
- Mencap/RNIB seed columns `la_code` hold the literal `NA`; kept for parity, tests allow it.
- Snowflake `T2 = '999.0'` for Scope is a numeric round-trip artefact; dbt writes `'999'`.
- Carers UK `full_series` tables, Age UK full-series pair as primary source, per-topic fallback and the 3% hard stop belong to `reparse/history-from-raw`; not implemented (not on main).

## Gaps

Mart isolation check not run (limit). Age UK, Scope not reproducible from current raw. Pre-coverage months exist only in Snowflake. Pairing of Age UK files depends on Fivetran table names. Source tables are discovered at run time, so `dbt ls` shows no lineage to raw.

## 2026-10-16 changes (owner decisions)

- Mart renamed `helplines_aggregated_full` to `helplines_ongoing` (the State of the Nation output will be promoted alongside it). The Snowflake source keeps its table name.
- Stable period stays at 5 partners (as `data_discriminator.R`). 6 was tried (window shrank to 2024-10..2025-12, 165 rows) and reverted; the owner will revisit once the State of the Nation data is combined (see the tracking issue). Mart row-count floor test is 200.
- The ONS LAD, geography-to-LA and LA-to-CASSR lookups moved from dbt seeds to MotherDuck `reference.main` (`ons_lad_2025`, `geo_to_la`, `la_to_cassr`), published once by `scripts/seed/publish_la_reference.py`.

## Mart isolation check, diagnosed (2026-10-16)

Run: dbt SQL on Snowflake's own PARTNER_HISTORY_* vs the live HELPLINES_AGGREGATED_FULL (`scripts/parity/helplines_mart_check.py`, stable period 5 partners; details in `helplines_mart_check.md`).

- 790 vs 24,743 rows was a model bug, now fixed. `helplines_cassr_name(la_name)` is a correlated subquery over the lookup table, which has its own `la_name` column; the unqualified argument bound to the lookup's column instead of the outer row, so the CASSR lookup returned an arbitrary name or NULL and nearly every LA collapsed to NULL (790 rows is about 77 months x 9 UT1 + a few). The comparison key was not at fault. Fix: the mart joins the lookup instead of using the subquery (the macro now documents that its argument must be qualified; partner models already pass qualified columns).
- After the fix: 25,673 rows vs 24,743; sum(n) 676,229 vs 655,867 (+3.1%); the same 77 months; stable period 2020-01..2026-05.
- The remainder is Snowflake's mart being older than Snowflake's history, not a logic difference. Applying the mart rules to the same history reproduces the mart's monthly totals exactly for the 26 months 2020-01..2022-02. From 2023-04 the history holds more rows: +800..1,300 a month, which is the Citizens Advice switch from the first Tableau export to the fuller one (2023-04: 8,345 vs 9,381 in the raw files), reloaded into history after the mart was built. From 2025-03 the mart holds more than the history (-200..-480 a month: later reloads of Independent Age, Scope and Ealing). 88% of the surplus is Information Seeking on NULL LA_NAME (CAB, Carers UK, Ealing are region-level).
- Rows only on one side (1,650 dbt, 720 Snowflake) are keys whose count moved between the two vintages; none is a month or UT1 missing on one side.
- Open: rerun the check after the next Snowflake mart load to confirm it goes to PASS on identical vintages.
