# Parity: chatbot feature models, group A (int_chatbot_*)

- Date: 2026-10-15  |  frozen cutoff vs ACCESSAVA: **2026-10-13** (27345 ACCESSAVA transcripts)
- Oracle: Snowflake AVA.PUBLIC.ACCESSAVA (R's joined output, deduplicated by dedupe_by_id). Each column compared over all ACCESSAVA ids; a transcript missing from the dbt model counts as NULL.
- Cells are counts of ACCESSAVA transcripts. `diff` = both non-NULL and different; `only SF` / `only dbt` = one side NULL.

## chatbot_total_convos -> int_chatbot_total_convos

- dbt rows 27413, distinct transcript_id 27413, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| created_at | 0 | 27345 | 0 | 0 | 0 |
| tenant_name | 0 | 27345 | 0 | 0 | 0 |
| engaged | 0 | 27345 | 0 | 0 | 0 |
| region | 26591 | 754 | 0 | 0 | 0 |
| organisation_type | 25260 | 2085 | 0 | 0 | 0 |

## chatbot_completions -> int_chatbot_completions

- dbt rows 2536, distinct transcript_id 2536, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| completed | 24814 | 2531 | 0 | 0 | 0 |

## chatbot_ethnicity -> int_chatbot_ethnicity

- dbt rows 239, distinct transcript_id 239, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| ethnicity | 26946 | 239 | 160 | 0 | 0 |

`ethnicity`: of the 160 `only SF` values, 160 are transcripts whose value comes from the external equalities form (process_raw_data coalesces demographics_matched.RDS in).

## chatbot_age_group -> int_chatbot_age_group

- dbt rows 259, distinct transcript_id 259, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| age_range | 26926 | 259 | 160 | 0 | 0 |

`age_range`: of the 160 `only SF` values, 160 are transcripts whose value comes from the external equalities form (process_raw_data coalesces demographics_matched.RDS in).

## chatbot_person_type -> int_chatbot_person_type

- dbt rows 255, distinct transcript_id 255, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| person_type | 27090 | 255 | 0 | 0 | 0 |

## chatbot_disability -> int_chatbot_disability

- dbt rows 242, distinct transcript_id 242, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| disability_n_y | 26943 | 242 | 160 | 0 | 0 |

`disability_n_y`: of the 160 `only SF` values, 160 are transcripts whose value comes from the external equalities form (process_raw_data coalesces demographics_matched.RDS in).
| disability_text | 27235 | 110 | 0 | 0 | 0 |

## chatbot_armed_forces -> int_chatbot_armed_forces

- dbt rows 252, distinct transcript_id 252, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| armed_service | 27093 | 252 | 0 | 0 | 0 |

## chatbot_la -> int_chatbot_la

- dbt rows 3236, distinct transcript_id 3236, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| la_name | 24317 | 3028 | 0 | 0 | 0 |

## chatbot_time_spent -> int_chatbot_time_spent

- dbt rows 27409, distinct transcript_id 27409, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| dif | 4 | 27341 | 0 | 0 | 0 |

## chatbot_churn -> int_chatbot_churn

- dbt rows 27409, distinct transcript_id 27409, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| churn_point | 4 | 27341 | 0 | 0 | 0 |
| churn_category | 4 | 27341 | 0 | 0 | 0 |

## chatbot_ratings -> int_chatbot_ratings

- dbt rows 1026, distinct transcript_id 1026, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| rating | 26320 | 1025 | 0 | 0 | 0 |

## chatbot_feedback -> int_chatbot_feedback

- dbt rows 360, distinct transcript_id 360, duplicate rows 0

| column | both NULL | equal | only SF | only dbt | diff |
|---|---|---|---|---|---|
| user_feedback | 26989 | 356 | 0 | 0 | 0 |

## Findings

- All 12 functions match ACCESSAVA on every column at the cutoff, except `only SF` ethnicity / age_range / disability_n_y (160 each): all 160 are transcripts in the external equalities form (ascFuncs demographics_matched.RDS, 170 rows), which `process_raw_data` coalesces in. The join model needs that data as a seed.
- Not compared: the R chain itself (ACCESSAVA only). '' and NULL are not distinguished (CSV pull).
- R bug mirrored: `chatbot_person_type` sorts by created_at DESC before `lag()`, so the value is the bot message after the question (249 of 255 ACCESSAVA values are "Thanks! Remember you can skip...", 6 are "Next"). `--vars '{chatbot_person_type_fix_order: true}'` gives the real answers (232 transcripts; 4 values). Default keeps parity.
- R quirk mirrored: `chatbot_feedback` applies its "same as previous row" filter after `ungroup()`, so it compares across transcripts in global id order. That accidentally keeps one row per feedback; a within-transcript lag would keep 7 transcripts instead of 360. Result depends on which transcripts are present (4 of 364 dropped at the cutoff).
- Deliberate: empty-string answers are dropped (ACCESSAVA reads data.csv with '' as NA and dedupe keeps non-NA rows); ethnicity and age_range keep the latest answer per transcript (R returns duplicates, dedupe_by_id keeps the last).
- Porting fixes found by parity: `str_trim` strips non-breaking spaces (RE2 `\s` does not; 131 churn_point values); region/organisation_type are tenant group subgroups pivoted as in chatbot_get_secondary_tables.
- `completed` from trackers: R's pattern is the Contentful texts joined with `|`; the safeguarding text contains `$` so in practice only "Download letter" matches.
- Cost: la (~2 min) and feedback (~1 min) scan the wide raw_record column on MotherDuck.

Reproduce: `python scripts/parity/chatbot_features_a_parity.py --cutoff 2026-10-13 --form-csv form.csv --out docs/parity/chatbot_features_a.md` (rerunning overwrites this Findings section).
