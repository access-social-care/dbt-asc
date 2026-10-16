# Parity: chatbot feature models, group B

- Date: 2026-10-15  |  frozen cutoff: **2026-10-13**  |  oracle: AVA.PUBLIC.ACCESSAVA (27345 conversations)
- Per column, over ACCESSAVA conversations: model value (NULL when the model has no row) vs ACCESSAVA value.
- 'dbt-only ids' = feature rows for conversations in the dbt id universe but not in ACCESSAVA at the cutoff.

| model.column | ACCESSAVA non-null | dbt non-null | equal | differ | ACCESSAVA only | dbt only | dbt-only ids |
|---|---|---|---|---|---|---|---|
| int_chatbot_persona.persona | 11226 | 11226 | 11226 | 0 | 0 | 0 | 0 |
| int_chatbot_s_c_csi.supercategories | 3442 | 3442 | 3442 | 0 | 0 | 0 | 0 |
| int_chatbot_s_c_csi.categories | 2745 | 2745 | 2745 | 0 | 0 | 0 | 0 |
| int_chatbot_s_c_csi.csis | 1461 | 1461 | 1461 | 0 | 0 | 0 | 0 |
| int_chatbot_letters_downloaded.lettercode | 2393 | 2393 | 2393 | 0 | 0 | 0 | 0 |
| int_chatbot_letters_downloaded.letter_downloaded | 2473 | 2473 | 2473 | 0 | 0 | 0 | 0 |
| int_chatbot_autocomplete_starts.autocomplete_start | 779 | 781 | 779 | 0 | 0 | 2 | 0 |
| int_chatbot_autocomplete_finish.autocomplete_finish | 266 | 266 | 266 | 0 | 0 | 0 | 0 |
| int_chatbot_factsheets_templates.factsheets_templates | 547 | 547 | 547 | 0 | 0 | 0 | 0 |
| int_chatbot_rasa.rasa | 287 | 288 | 287 | 0 | 0 | 1 | 0 |
| int_chatbot_topic_entry_point.topic_entry_point | 14877 | 14877 | 14877 | 0 | 0 | 0 | 0 |
| int_chatbot_consumed_resources.consumed_resources | 3480 | 3480 | 3480 | 0 | 0 | 0 | 0 |
| int_chatbot_age_of_person.age | 4510 | 4510 | 4510 | 0 | 0 | 0 | 0 |
| int_chatbot_conversation_source.referring_page_url | 24940 | 24940 | 24940 | 0 | 0 | 0 | 0 |
| int_chatbot_conversation_source.referring_page_parameters | 12023 | 12023 | 12023 | 0 | 0 | 0 | 0 |

## Findings

- 13 of 15 compared columns are exact (0 differ, 0 one-sided) on all 27345 ACCESSAVA conversations at the cutoff. Persona, supercategories, categories, csis, lettercode, topic_entry_point, consumed_resources and age match on value as well as presence, so ordering and delimiters ('; ' for persona/topic/supercategories/categories/csis; ';' for lettercode, consumed_resources) are byte-identical to R.
- autocomplete_start (2 transcripts) and rasa (1 of the same 2): dbt flags TRUE where ACCESSAVA is NULL. Both are 2023 sessions whose raw_tracker_record in Fivetran contains the phrase; the R tr_autocomplete_starts.fst / tr_rasa.fst snapshots (Feb 2026 extraction from the then-cached tracker) did not. Root cause not provable without the R cache; dbt reads the tracker live, so it is the more complete answer. No other tracker-derived flag differs (letters 2473, factsheets 547, finish 266 exact).
- The tracker regexes (Download letter, Factsheets & Template letters, Ask a specific question, Let me help you write a letter or email, Thank you for answering the questions $UserName.) are plain-text matches and work on the Fivetran JSON-array format. Only 445 transcripts (2023-03-14 to 2023-06-19) have a tracker; tracker-derived flags cannot change for newer data.
- 'NA' handling: R paste() renders missing values as the text NA and ACCESSAVA reads a lone 'NA' back as missing (data.csv round trip). The s_c_csi columns are NULL when the whole value is 'NA'; mixed lists such as 'x; NA' are kept (they match ACCESSAVA). Same for empty referring_page_parameters (ACCESSAVA NULL, not '').
- Fivetran soft deletes: transcript b46e3c19-7a47-420b-b34b-9b85620be2ed has a live row and a newer soft-deleted twin in raw actions_transcript. Picking the latest row then dropping deleted rows (the base-model pattern) loses it; conversation_source drops deleted rows first. The same pattern in stg_chatbot_transcript means that transcript has no tr row in the base (it still has events, so it is in ACCESSAVA).
- age: R keeps the LAST of the distinct age answers in first-seen order, not the last answer chronologically (A, B, A gives B). Mirrored, not fixed.
- category_map (ascFuncs package dataset, 672 distinct rows) is embedded in int_chatbot_category_map; regenerate when ascFuncs ships a new one.
- Out of scope (not in the process_raw_data join): chatbot_popular_factsheets_templates, chatbot_user_paths, chatbot_traffic_by_tenant, chatbot_monthly_web_hits.
- Feature models are not filtered to is_reporting_tenant; the join model filters (parity above is on ACCESSAVA ids, which are reporting tenants only).

Reproduce: `python scripts/parity/chatbot_features_b_parity.py --cutoff 2026-10-13 --out docs/parity/chatbot_features_b.md` (then re-append this section).
