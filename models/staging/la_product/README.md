# LA Data Product — Staging Layer

Two parallel tracks produce data at different segment granularities. Choose the right base model depending on whether you need raw source categories or cross-source UT1 comparison.

---

## Track 1 — Raw category (stg_la_queries)

**Use when:** analysing each source's own category taxonomy. No helplines.

| Model | Source | SEGMENT |
|---|---|---|
| `stg_accessava` | `accessava.accessava` | `categories` exploded on `'; '` |
| `stg_advicepro` | `casework.advicepro_casework` | `case_specific_issues_group` exploded on `';'` |

Unioned in **`stg_la_queries`**. Grain: one row per conversation/case × category value.

---

## Track 2 — UT1 mapped (stg_la_queries_segments)

**Use when:** comparing across AccessAva, AdvicePro, and Helplines using the shared Universal Theme taxonomy. All `mart_glos_*` tables use this track.

| Model | Source | SEGMENT | Extra joins |
|---|---|---|---|
| `stg_accessava_segments` | `accessava.accessava` | `topic_entry_point` → `topic_entry_point_map` → UT1 | `accessava_locality` |
| `stg_advicepro_segments` | `casework.advicepro_casework` | `case_topic_bridge` → `s_c_csi_map` → `universal_themes_map` → UT1 | `advicepro_demographics`, `casework_locality` |
| `stg_helplines` | `helplines.helplines_aggregated_full` | UT1 natively (pre-aggregated) | — |

Unioned in **`stg_la_queries_segments`**. Grain: one row per conversation/case × topic mention.

`stg_la_queries_glos` filters `stg_la_queries_segments` to Gloucestershire and is the base for all `int_glos_*` and `mart_glos_*` models.

---

## QUERY_COUNT semantics

In both tracks, `QUERY_COUNT = 1` per row. Because rows are at topic-mention grain (not conversation grain), **summing QUERY_COUNT gives topic mentions, not queries handled.** A conversation touching 3 topics contributes 3. This is intentional — document it in any report using these models.

---

## Null availability by source

| Column | AccessAva | AdvicePro | Helplines |
|---|---|---|---|
| LOCALITY_NAME | ward (where resolved) | ward (where resolved) | NULL |
| AGE_BAND | age band (where recorded) | age_range (where recorded) | NULL |
| HAS_LETTER | 0 or 1 | 0 always | NULL |
