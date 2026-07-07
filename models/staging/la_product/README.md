# LA Data Product — Staging Layer

All models use **UT1/UT2-mapped topic mentions** — the only track. Source-specific raw-category staging models (the old "Track 1") were removed; `stg_la_topic_mentions` is the canonical entry point for all analysis.

---

## Staging models

| Model | Source | SEGMENT | Notes |
|---|---|---|---|
| `stg_accessava` | `accessava.accessava` | `topic_entry_point` → `topic_entry_point_map` → UT1/UT2 | Flattens semicolon-joined topics; joins `accessava_locality` for county |
| `stg_advicepro` | `casework.advicepro_casework` | `case_topic_bridge` → `s_c_csi_map` → `universal_themes_map` → UT1/UT2 | Joins `casework_locality` for county; `advicepro_demographics` for age |
| `stg_helplines` | `helplines.helplines_aggregated_full` | UT1 natively (pre-aggregated by ETL) | No UT2, locality, age, or letter dimensions |

Unioned in **`stg_la_topic_mentions`**. Grain: one row per conversation/case × topic mention — a conversation touching 3 topics contributes 3 rows. Named for this grain, not "queries": one query/interaction is not one row.

`stg_la_topic_mentions_glos` filters `stg_la_topic_mentions` to Gloucestershire and is the base for all `int_glos_*` (in `models/intermediate/la_product/`) and `mart_glos_*` models.

---

## Lineage

```
stg_accessava  ─┐
stg_advicepro  ─┼─> stg_la_topic_mentions ─┬─> stg_la_topic_mentions_glos ─> int_glos_* ─> mart_glos_*
stg_helplines  ─┘                          ├─> mart_la_query_summary  (all LAs, all-time)
                                            └─> helplines_advicepro_accessava  (monthly UT1/UT2, models/staging/acs_helplines/)
```

`int_glos_*` models live in `models/intermediate/la_product/` (not this folder) — same physical Snowflake schema (`la_product_staging`), just organised separately from source-level staging per dbt convention.

---

## QUERY_COUNT semantics

`QUERY_COUNT = 1` per row. Because rows are at topic-mention grain (not conversation grain), **summing QUERY_COUNT gives topic mentions, not queries handled.** A conversation touching 3 topics contributes 3. Document this in any report using these models.

---

## Null availability by source

| Column | AccessAva | AdvicePro | Helplines |
|---|---|---|---|
| UT2 | populated where mapped | populated where mapped | always NULL |
| LOCALITY_NAME | county (where resolved by postcode lookup) | county (where resolved by postcode lookup) | NULL |
| AGE_BAND | age band (where recorded) | age_range (where recorded) | NULL |
| HAS_LETTER | 0 or 1 | 0 always | NULL |

**Note on LOCALITY_NAME / county:** AdvicePro county comes from `CASEWORK.PUBLIC.CASEWORK_LOCALITY` loaded by `loaders/load_casework_locality_to_snowflake.R` (findthatpostcode.uk). AccessAva county comes from `ACCESSAVA.PUBLIC.ACCESSAVA_LOCALITY` loaded by `chatbot_data/data_uploader.R` — `county` and `county_code` confirmed present in both loaders (2026-07).

---

## Production status

All `mart_glos_*` tables are in production, serving the Gloucestershire LA data product. `mart_la_query_summary` is a cross-LA all-time aggregate (used for ad-hoc analysis). `helplines_advicepro_accessava` (in `models/staging/acs_helplines/`, `staging_acs_helplines` schema) reads from `stg_la_topic_mentions` and collapses to monthly UT1/UT2 grain — no known consumers as of 2026-07.
