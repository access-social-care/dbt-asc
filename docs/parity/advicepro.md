# Parity: AdvicePro casework (raw.advicepro.cases_raw) vs CASEWORK.PUBLIC

Date: 2026-10-16. Compared against production `CASEWORK.PUBLIC` (ROLE_DEV can
SELECT it; `CASEWORK_DEV` was not used). All checks are SELECT-only, aggregates
pushed down via `scripts/parity/parity.py`, plus an in-memory row-level diff
whose results are summarised here as counts. No case-level values are recorded.

## What replaces what

`case_data/load_advicepro_to_snowflake.R` (transform half) becomes dbt models
over `raw.advicepro.cases_raw`:

| Snowflake table | dbt model (marts.main unless noted) |
|---|---|
| ADVICEPRO_CASEWORK | `advicepro_casework` |
| CASE_TOPIC_BRIDGE | `case_topic_bridge` |
| SUPER_CATEGORY_BRIDGE / _DIM | `advicepro_super_category_bridge` / `_dim` |
| CSI_GROUP_BRIDGE / _DIM | `advicepro_csi_group_bridge` / `_dim` |
| CSI_BRIDGE / _DIM | `advicepro_csi_bridge` / `_dim` |
| OUTCOME_BRIDGE / _DIM | `advicepro_outcome_bridge` / `_dim` |
| MEMBER_NAME_MAPPING | seed `member_name_mapping` (staging.main) |

The `advicepro_` prefix avoids clashing with the chatbot `csi_bridge` and
`csi_dim` already in `marts.main`. Staging: `stg_advicepro_cases`,
`int_advicepro_la_lookup`, `int_advicepro_case_topics`, `int_advicepro_cases`.

## Result

| Table | Verdict | Detail |
|---|---|---|
| advicepro_casework | DIFFERS (explained) | 2,829 rows vs 2,829, key set identical. 11 of 16 columns identical (references, months, status, member, super_category, outcome, both dates). Differences below. |
| case_topic_bridge | DIFFERS (explained) | 2,368 vs 3,628 rows. Identical on every case that has an AdvicePro topic; 1,260 extra Snowflake rows belong to 868 Dynamics-backfilled cases. |
| advicepro_super_category_bridge / _dim | PARITY | 2,829 / 2 rows, key sets identical. |
| advicepro_outcome_bridge / _dim | PARITY | 2,382 / 23 rows, key sets identical. |
| member_name_mapping (seed) | PARITY | 73 rows identical. |
| advicepro_csi_group_bridge | DIFFERS (explained) | 2,210 vs 3,425; identical on topic-bearing cases, 1,215 extra Snowflake rows for the same 868 cases. |
| advicepro_csi_group_dim | DIFFERS (explained) | 64 vs 73 values; all 64 are in Snowflake, 9 exist only via Dynamics. Surrogate ids shift as a result. |
| advicepro_csi_bridge | DIFFERS (explained) | 2,367 vs 3,624; identical on topic-bearing cases, 1,257 extra rows for the same 868 cases. |
| advicepro_csi_dim | DIFFERS (explained) | 202 vs 235 values; all 202 are in Snowflake, 33 exist only via Dynamics. Surrogate ids shift. |

### advicepro_casework differences, quantified

| Column | Cases differing | Cause |
|---|---|---|
| case_level | all (NULL here; 2,806 populated in Snowflake) | Not in the landed report (`landing_log.columns_dropped` = `case_level;number_of_cases`). Cannot be reproduced. |
| la_code, la_name | 23 (NULL here, value in Snowflake) | Dynamics backfill fills LA only where AdvicePro has none. All 23 have an LA in `DYNAMICS_CASE_MIGRATION`. |
| case_specific_issues_group, case_specific_issues | 868 (NULL here, value in Snowflake) | Dynamics backfill supplies topics to cases with no AdvicePro topic. 867 have a first-slot Dynamics topic, 1 has only slot 2 or 3. The other 1,101 - 868 = 233 cases stay topic-less in both. |

Total cases differing on any of these: 884. Every other case, including all
1,728 that have an AdvicePro topic, matches exactly, including the collapsed
string order.

Dynamics-only data (`CASEWORK_STAGING.PUBLIC.DYNAMICS_CASE_MIGRATION`) is not in
MotherDuck, so the backfill is excluded. Charter says it takes effect only
where AdvicePro has no topic or no LA, so the AdvicePro-native part is
unaffected.

## Normalisation details worth knowing

- Collapsed topic strings sort case-insensitively (`lower(v)`, then lower-case
  first). Binary order gave 7 mismatching cases; base R `sort()` on the VM uses a
  locale collation.
- Surrogate keys in dims use binary order (dplyr `arrange`), as the loader does.
- Local authority: the three-step ascFuncs standardiser (ONS LAD, ONS geography,
  alias map) is reproduced from `reference.main.ons_lad_2025`, `geo_to_la`,
  `la_name_map`, then `la_to_cassr`. 9 distinct raw values do not match any
  ONS name or alias (warn test); la_name is NULL for any value with no CASSR.

## Tests (replacing the loader's guards)

| Loader guard | dbt test |
|---|---|
| Uniqueness after collapse | `unique` + `not_null` on case_reference (int and mart) |
| Unknown group prefix aborts | `not_null` on `int_advicepro_case_topics.supercat_canon` (error) |
| Duplicate S_C_CSI_MAP keys abort | singular test `assert_s_c_csi_map_keys_unique` (error) |
| Case-insensitive dim duplicates abort (CSI_DIM warns) | generic `unique_case_insensitive` (error; warn on csi dim) |
| Taxonomy drift logged | `not_null` on `case_topic_bridge.s_c_csi_id` (warn) |
| Unmatched LA reported | `not_null` on `int_advicepro_la_lookup.la_code` (warn) |
| Bridge to dim keys | `relationships` on every bridge (case and value) |
| Member mapping | `unique` on `advicepro_name` where not null (seed) |

Current run: 50 tests, 46 pass, 4 warn (2 drift pairs, 9 unmatched LA values,
1 native case variant in the specific-issue dim), 0 error.

## Downstream wiring

`casework_relation(name)` returns `ref(name)` on DuckDB and
`source('casework', name)` on Snowflake. Wired into `stg_advicepro`,
`stg_ilta_advicepro` and the `ut1_groundtruth_feb2026_glos` analysis.

Both staging models stay disabled on DuckDB: they also join
`advicepro_demographics`, and `stg_advicepro` joins `casework_locality`;
neither exists in MotherDuck (separate loaders, separate AdvicePro reports).
Everything downstream (`stg_la_topic_mentions*`, `mart_ilta_demographics`, the
`la_product` and `analytics` marts, `helplines_advicepro_accessava`) is blocked
by those two, and the cross-source ones also by helplines and AccessAva
inputs. No existing model needs casework alone.

## Reference candidates

- `member_name_mapping` seed (73 rows, source of truth stays
  `case_data/config/member_name_mapping_seed.csv`).
- `advicepro_prefix_supercategory` seed (12 rows, the loader's `prefix_to_supercat`).
