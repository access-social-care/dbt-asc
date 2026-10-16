# Parity: chatbot star layer (accessava, bridges, dims, locality, downstream)

- Date: 2026-10-16  |  frozen cutoff vs Snowflake: **2026-10-13** (27345 ACCESSAVA conversations)
- Oracle: Snowflake AVA.PUBLIC.* and ANALYTICS staging, SELECT only. No data rows in this file.
- Verdict legend: PARITY = every row and column equal at the cutoff. Rows: `rows in both` joined on the key.

## Summary

- accessava: row set and 33 of 36 columns exact. Expected differences: autocomplete_start (2) and rasa (1), dbt flags TRUE where ACCESSAVA is NULL (tracker phrase present in Fivetran, absent in the R snapshot); all three are the 2023 sessions already found in group B. Excluded: `...1` (write.csv row-number artifact of the CSV hop).
- Bridges and dims (7 pairs): PARITY, surrogate keys identical.
- accessava_locality: PARITY.
- stg_ilta_accessava, stg_accessava: PARITY at the cutoff. tenant marts: PARITY on the re-run aggregation (the two mart tables are not materialised in Snowflake).
- Base soft-delete fix: transcript b46e3c19-7a47-420b-b34b-9b85620be2ed recovered (+1 stg_chatbot_transcript row; it was already in accessava through its events). It was the only id of 4,754,645 with a live row plus a soft-deleted twin; no events are affected.

## Findings

- Fivetran keys actions_transcript by ctid (no primary key), so a Postgres update lands as a new live row and a soft-deleted twin of the old version, and the twin can have the newer _fivetran_synced. Rule: drop soft-deleted rows first, then keep the latest row per id.
- person_type bug is mirrored (int_chatbot_person_type default); the flag chatbot_person_type_fix_order gives real answers.
- Persona surrogate key: `add_surrogate_key(persona$dim, "persona_id", persona)` passes the list variable, but the function takes the argument with `rlang::ensym()`, which captures the symbol `persona` and resolves it as the column inside the data frame. It is not a bug: PERSONA_DIM holds 1, 2, 3 with ids 1, 2, 3 and the dbt dim is identical.
- Delimiters: data_uploader.R splits every attribute on `;` with optional surrounding whitespace and squishes tokens, so '; ' and ';' columns are handled by one rule. The report.Rmd `gsub('; ', ',')` and persona relabel run only on the Monday export, after data.csv is written; they never reach ACCESSAVA.
- Dim keys: binary (C-locale) sort of distinct values, row_number from 1. SUPERCATEGORY_DIM contains the literal value 'NA' (from mixed lists), as in Snowflake.
- Bridges are not distinct (a value repeated in one list repeats in the bridge), as in R; the row-number comparison covers duplicates.
- The dedupe_by_id step of prepare_accessava is a no-op in dbt: every feature model is one row per transcript_id (unique tests).
- ACCESSAVA `county` is NULL for all rows in Snowflake (stale export column); kept as NULL for column parity.
- Form demographics: 170 rows seeded by scripts/seed/seed_form_demographics.py into analytics_dev.landing.chatbot_form_demographics; nothing derived from the RDS is in the repo. Flag for quality-officer review.
- stg_chatbot_transcript_event soft-delete change was not rebuilt (zero affected ids; a rebuild scans raw_record).

## ACCESSAVA

### accessava vs AVA.PUBLIC.ACCESSAVA (created_at <= 2026-10-13): DIFFERS

- rows in both: 27345; only Snowflake: 0; only dbt: 0

| column | both NULL | equal | diff | only SF | only dbt |
|---|---|---|---|---|---|
| created_at | 0 | 27345 | 0 | 0 | 0 |
| tenant_name | 0 | 27345 | 0 | 0 | 0 |
| engaged | 0 | 27345 | 0 | 0 | 0 |
| region | 26591 | 754 | 0 | 0 | 0 |
| county | 27345 | 0 | 0 | 0 | 0 |
| organisation_type | 25260 | 2085 | 0 | 0 | 0 |
| completed | 24814 | 2531 | 0 | 0 | 0 |
| ethnicity | 26946 | 399 | 0 | 0 | 0 |
| age_range | 26926 | 419 | 0 | 0 | 0 |
| person_type | 27090 | 255 | 0 | 0 | 0 |
| disability_n_y | 26943 | 402 | 0 | 0 | 0 |
| disability_text | 27235 | 110 | 0 | 0 | 0 |
| armed_service | 27093 | 252 | 0 | 0 | 0 |
| la_name | 24317 | 3028 | 0 | 0 | 0 |
| dif | 4 | 27341 | 0 | 0 | 0 |
| churn_point | 4 | 27341 | 0 | 0 | 0 |
| churn_category | 4 | 27341 | 0 | 0 | 0 |
| rating | 26320 | 1025 | 0 | 0 | 0 |
| user_feedback | 26989 | 356 | 0 | 0 | 0 |
| persona | 16119 | 11226 | 0 | 0 | 0 |
| supercategories | 23903 | 3442 | 0 | 0 | 0 |
| categories | 24600 | 2745 | 0 | 0 | 0 |
| csis | 25884 | 1461 | 0 | 0 | 0 |
| lettercode | 24952 | 2393 | 0 | 0 | 0 |
| letter_downloaded | 24872 | 2473 | 0 | 0 | 0 |
| autocomplete_start | 26564 | 779 | 0 | 0 | 2 |
| autocomplete_finish | 27079 | 266 | 0 | 0 | 0 |
| factsheets_templates | 26798 | 547 | 0 | 0 | 0 |
| rasa | 27057 | 287 | 0 | 0 | 1 |
| topic_entry_point | 12468 | 14877 | 0 | 0 | 0 |
| consumed_resources | 23865 | 3480 | 0 | 0 | 0 |
| age | 22835 | 4510 | 0 | 0 | 0 |
| referring_page_url | 2405 | 24940 | 0 | 0 | 0 |
| referring_page_parameters | 15322 | 12023 | 0 | 0 | 0 |
| entry_month | 0 | 27345 | 0 | 0 | 0 |

## Bridges and dimensions

| object | verdict | rows both | only SF | only dbt | note |
|---|---|---|---|---|---|
| PERSONA_BRIDGE | PARITY | 13119 | 0 | 0 | cutoff 2026-10-13 |
| PERSONA_DIM | PARITY | 3 | 0 | 0 | surrogate key diff 0 |
| SUPERCATEGORY_BRIDGE | PARITY | 3979 | 0 | 0 | cutoff 2026-10-13 |
| SUPERCATEGORY_DIM | PARITY | 5 | 0 | 0 | surrogate key diff 0 |
| CATEGORY_BRIDGE | PARITY | 3282 | 0 | 0 | cutoff 2026-10-13 |
| CATEGORY_DIM | PARITY | 15 | 0 | 0 | surrogate key diff 0 |
| CSI_BRIDGE | PARITY | 1998 | 0 | 0 | cutoff 2026-10-13 |
| CSI_DIM | PARITY | 17 | 0 | 0 | surrogate key diff 0 |
| TOPIC_ENTRY_POINT_BRIDGE | PARITY | 29874 | 0 | 0 | cutoff 2026-10-13 |
| TOPIC_ENTRY_POINT_DIM | PARITY | 27 | 0 | 0 | surrogate key diff 0 |
| LETTER_CODE_BRIDGE | PARITY | 3053 | 0 | 0 | cutoff 2026-10-13 |
| LETTER_CODE_DIM | PARITY | 255 | 0 | 0 | surrogate key diff 0 |
| CONSUMED_RESOURCE_BRIDGE | PARITY | 5099 | 0 | 0 | cutoff 2026-10-13 |
| CONSUMED_RESOURCE_DIM | PARITY | 138 | 0 | 0 | surrogate key diff 0 |

## Locality

### accessava_locality vs AVA.PUBLIC.ACCESSAVA_LOCALITY (transcripts in ACCESSAVA at 2026-10-13): PARITY

- rows in both: 433; only Snowflake: 0; only dbt: 0

| column | both NULL | equal | diff | only SF | only dbt |
|---|---|---|---|---|---|
| la_name | 0 | 433 | 0 | 0 | 0 |
| local_authority_code | 0 | 433 | 0 | 0 | 0 |
| ward | 0 | 433 | 0 | 0 | 0 |
| ward_code | 0 | 433 | 0 | 0 | 0 |
| constituency_name | 0 | 433 | 0 | 0 | 0 |
| constituency_code | 0 | 433 | 0 | 0 | 0 |
| mso_area_name | 0 | 433 | 0 | 0 | 0 |
| mso_area_code | 0 | 433 | 0 | 0 | 0 |
| lso_area_name | 0 | 433 | 0 | 0 | 0 |
| lso_area_code | 0 | 433 | 0 | 0 | 0 |
| nhs_icb_name | 0 | 433 | 0 | 0 | 0 |
| nhs_icb_code | 0 | 433 | 0 | 0 | 0 |
| urban_rural_classification_name | 0 | 433 | 0 | 0 | 0 |
| urban_rural_classification_code | 0 | 433 | 0 | 0 | 0 |
| urban_conurbation_classification_name | 0 | 433 | 0 | 0 | 0 |
| urban_conurbation_classification_code | 0 | 433 | 0 | 0 | 0 |
| county | 278 | 155 | 0 | 0 | 0 |
| county_code | 278 | 155 | 0 | 0 | 0 |

## Downstream models

### mart_chatbot_conversations_by_tenant_monthly vs model SQL on AVA.PUBLIC.ACCESSAVA (months before 2026-10-01): PARITY

- rows in both: 863; only Snowflake: 0; only dbt: 0

| column | both NULL | equal | diff | only SF | only dbt |
|---|---|---|---|---|---|
| conversation_count | 0 | 863 | 0 | 0 | 0 |
| unique_conversations | 0 | 863 | 0 | 0 | 0 |
| first_conversation_date | 0 | 863 | 0 | 0 | 0 |
| last_conversation_date | 0 | 863 | 0 | 0 | 0 |

### tenant_total logic, cutoff 2026-10-13 (aggregation re-run both sides; the model itself is the same SQL as marts.mart_chatbot_conversations_by_tenant_total): PARITY

- rows in both: 51; only Snowflake: 0; only dbt: 0

| column | both NULL | equal | diff | only SF | only dbt |
|---|---|---|---|---|---|
| total_conversations | 0 | 51 | 0 | 0 | 0 |
| unique_conversations | 0 | 51 | 0 | 0 | 0 |
| first_conversation_date | 0 | 51 | 0 | 0 | 0 |
| last_conversation_date | 0 | 51 | 0 | 0 | 0 |
| days_active | 0 | 51 | 0 | 0 | 0 |

### stg_ilta_accessava vs ANALYTICS.PUBLIC_ILTA_STAGING.STG_ILTA_ACCESSAVA (conversations in ACCESSAVA at 2026-10-13): PARITY

- rows in both: 27345; only Snowflake: 0; only dbt: 0

| column | both NULL | equal | diff | only SF | only dbt |
|---|---|---|---|---|---|
| source_system | 0 | 27345 | 0 | 0 | 0 |
| month_date | 0 | 27345 | 0 | 0 | 0 |
| la_name | 24317 | 3028 | 0 | 0 | 0 |
| org_name | 0 | 27345 | 0 | 0 | 0 |
| gender | 0 | 27345 | 0 | 0 | 0 |
| age | 0 | 27345 | 0 | 0 | 0 |
| disability | 0 | 27345 | 0 | 0 | 0 |
| ethnicity | 0 | 27345 | 0 | 0 | 0 |
| gender_raw | 27345 | 0 | 0 | 0 | 0 |
| age_raw | 26926 | 419 | 0 | 0 | 0 |
| disability_yn_raw | 26943 | 402 | 0 | 0 | 0 |
| disability_type_raw | 27235 | 110 | 0 | 0 | 0 |
| ethnicity_raw | 26946 | 399 | 0 | 0 | 0 |

### stg_accessava vs ANALYTICS.PUBLIC_LA_PRODUCT_STAGING.STG_ACCESSAVA (query_date <= 2026-10-13, full-row multiset): PARITY

- rows in both: 42342; only Snowflake: 0; only dbt: 0

Reproduce: `python scripts/parity/chatbot_star_parity.py --cutoff 2026-10-13 --part accessava|star|locality|marts`.
