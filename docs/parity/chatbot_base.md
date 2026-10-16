# Parity: chatbot base layer (stg_chatbot_transcript, stg_chatbot_transcript_event)

- Date: 2026-10-15  |  frozen entry_day cutoff vs ACCESSAVA: **2026-10-13**

## A. Base vs Snowflake AVA.PUBLIC.ACCESSAVA

- ACCESSAVA conversations (created_at <= cutoff): 27345
- Base-derived conversations: 27345 (distinct transcripts in stg_chatbot_transcript_event: 27341; engaged transcripts in stg_chatbot_transcript: 442; reporting tenants only)
- transcript_id in ACCESSAVA only: **0**; in base only: **0**; in both: 27345
- On common ids: tenant_name differs 0; created_at date differs 0
- Tenant x month cells: 885 compared, 885 equal, 0 differ

## B. Base vs the R pipeline's own tr / tr_ev (cached RDS, created_at < 2026-02-18)

### stg_chatbot_transcript vs chatbot_process_tr output

- Rows: R 3496811, dbt 3567035; in R only: **0**; in dbt only: **70224** (of which empty-tracker sessions: 70215)
- On common ids, mismatches: entry_month 0, entry_week 0, entry_day 0, tenant_name 247
- tenant_name differences (R cache vs now): 'Kent Austistic Trust' -> 'Kent Autistic Trust' (247)

### stg_chatbot_transcript_event vs chatbot_process_tr_ev output

- Rows: R 495580, dbt 495580; in R only: **0**; in dbt only: **0**
- Distinct transcript_id: R 18296, dbt 18296
- On common ids, mismatches: entry_month 0, entry_week 0, entry_day 0, tenant_name 49, contentful_id (other than R text "null" vs NULL) 0
- contentful_id: R text "null" where dbt has NULL on 172941 rows (deliberate, see model header)
- lettercode, rows R labelled non-NA: same 22780, different 254, dbt NULL 1349; R 'NA' but dbt has a code 38 (the R cache predates the current Contentful content)


## Findings behind the numbers

- ACCESSAVA parity is exact at the 2026-10-13 cutoff once the report.Rmd tenant exclusion (UAT, APDAS_SQUARESPACE; 22 transcripts) is applied via `is_reporting_tenant`. Fivetran lag is not visible at a cutoff two days back.
- R-cache gap, not a rule difference: the Feb 2026 R cache is missing 70,224 transcripts that Fivetran has (70,215 empty sessions, 9 real sessions with non-test names). All R rows are present in dbt; test-user removal gives identical event sets (0 differences in 495,580 events).
- Tracker format differs: RPostgres returned empty `raw_tracker_record` as `{}` and non-empty as a Postgres text-array literal of escaped JSON strings; Fivetran gives `[]` and a JSON array of objects. `is_engaged` treats both empties as empty. Regex/grep logic over raw_tracker_record in feature functions must be re-validated against the Fivetran format.
- The R `tr_total_convos.fst` is a static Feb 2026 snapshot; dbt derives the same set live (tr non-empty tracker union tr_ev first events).
- Deliberate deviations: contentful_id NULL instead of text "null"; lettercode follows current Contentful (R cache predates content renames).
- R chain was not re-run (it would overwrite the VM-style caches); comparison used the R cached tr/tr_ev outputs (2026-02-18/19).
- Contentful now lands natively in MotherDuck raw.contentful.contentful_data (scripts/seed/land_contentful_raw.py, stopgap until the chatbot_content QC job writes it); read via stg_contentful_data. Snowflake cannot be retired until that job writes MotherDuck.

Reproduce: `python scripts/parity/chatbot_base_parity.py --cutoff 2026-10-13 --r-tr r_tr.csv --r-trev r_trev.csv --r-cutoff 2026-02-18 --out docs/parity/chatbot_base.md` (CSVs from `Rscript scripts/parity/export_r_cache.R <cache_dir> <out_dir>`).