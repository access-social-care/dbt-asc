# LA Data Product — Staging Layer

All models use **Track 2 (UT1-mapped)** — the only track. Source-specific raw-category staging models were removed; use `stg_la_queries_segments` as the canonical entry point for all analysis.

---

## Staging models

| Model | Source | SEGMENT | Notes |
|---|---|---|---|
| `stg_accessava_segments` | `accessava.accessava` | `topic_entry_point` → `topic_entry_point_map` → UT1 | Flattens semicolon-joined topics; joins `accessava_locality` for county |
| `stg_advicepro_segments` | `casework.advicepro_casework` | `case_topic_bridge` → `s_c_csi_map` → `universal_themes_map` → UT1 | Joins `casework_locality` for county; `advicepro_demographics` for age |
| `stg_helplines` | `helplines.helplines_aggregated_full` | UT1 natively (pre-aggregated by ETL) | No locality, age, or letter dimensions |

Unioned in **`stg_la_queries_segments`**. Grain: one row per conversation/case × topic mention.

`stg_la_queries_glos` filters `stg_la_queries_segments` to Gloucestershire and is the base for all `int_glos_*` and `mart_glos_*` models.

---

## Intermediate and mart models

```
stg_la_queries_segments
└── stg_la_queries_glos  (Gloucestershire filter)
    ├── int_glos_la_activity_summary      -> mart_glos_la_activity_summary_{1m,3m,6m,9m,12m}
    ├── int_glos_la_demographics          -> mart_glos_la_demographics_{1m,3m,6m,9m,12m}
    ├── int_glos_la_legal_letters         -> mart_glos_la_legal_letters_{1m,3m,6m,9m,12m}
    ├── int_glos_la_locality_overview     -> mart_glos_la_locality_overview_{1m,3m,6m,9m,12m}
    ├── int_glos_la_query_segments        -> mart_glos_la_query_segments_{1m,3m,6m,9m,12m}
    ├── int_glos_la_queries_over_time     -> mart_glos_la_queries_over_time_{1m,3m,6m,9m,12m}
    └── int_glos_la_query_source          -> mart_glos_la_query_source_{1m,3m,6m,9m,12m}

stg_la_queries_segments (all LAs)
└── mart_la_query_summary                 (all-time aggregate, all LAs)
```

Each `int_glos_*` model pre-computes all 5 time windows (1m, 3m, 6m, 9m, 12m) in one table with a `TIME_WINDOW_MONTHS` column. The `mart_glos_*` models slice by time window and apply `la_suppress()` for small-number disclosure control.

---

## QUERY_COUNT semantics

`QUERY_COUNT = 1` per row. Because rows are at topic-mention grain (not conversation grain), **summing QUERY_COUNT gives topic mentions, not queries handled.** A conversation touching 3 topics contributes 3. Document this in any report using these models.

---

## Null availability by source

| Column | AccessAva | AdvicePro | Helplines |
|---|---|---|---|
| LOCALITY_NAME | county (where resolved by postcode lookup) | county (where resolved by postcode lookup) | NULL |
| AGE_BAND | age band (where recorded) | age_range (where recorded) | NULL |
| HAS_LETTER | 0 or 1 | 0 always | NULL |

**Note on LOCALITY_NAME / county:** AdvicePro county comes from `CASEWORK.PUBLIC.CASEWORK_LOCALITY` loaded by `loaders/load_casework_locality_to_snowflake.R` (findthatpostcode.uk). AccessAva county comes from `ACCESSAVA.PUBLIC.ACCESSAVA_LOCALITY` loaded by `chatbot_data/data_uploader.R` — confirm the `county` column is present in that loader before running `stg_accessava_segments` in production.

---

## Production status

All `mart_glos_*` tables are in production, serving the Gloucestershire LA data product. `mart_la_query_summary` is a cross-LA all-time aggregate (used for ad-hoc analysis). `helplines_advicepro_accessava` (in `staging_acs_helplines` schema) is a separate standalone model — monthly grain, reads directly from raw sources, not part of this lineage.
