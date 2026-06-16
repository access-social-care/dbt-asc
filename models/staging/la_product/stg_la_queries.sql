{{
  config(
    materialized='table',
    description='Combined LA query data: AccessAva + AdvicePro (UNION ALL of staging models)'
  )
}}

/*
  Stage 2 (raw-category track): UNION ALL of stg_advicepro + stg_accessava.

  SEGMENT in this track = raw source category value (not mapped to UT1):
    - stg_advicepro  → SEGMENT = case_specific_issues_group (exploded on ';')
    - stg_accessava  → SEGMENT = topic_entry_point (single value, not exploded)

  Use this model when you want analysis by each source's own category taxonomy.

  For cross-source comparison including Helplines, use stg_la_queries_segments instead:
    - SEGMENT = UT1 (universal theme, mapped for all three sources)
    - Helplines included
    - All mart_glos_* models read from stg_la_queries_segments via stg_la_queries_glos
*/

SELECT * FROM {{ ref('stg_advicepro') }}
UNION ALL
SELECT * FROM {{ ref('stg_accessava') }}
