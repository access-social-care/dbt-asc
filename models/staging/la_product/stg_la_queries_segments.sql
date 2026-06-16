{{
  config(
    materialized='table',
    description='Combined LA query data at topic-mention grain: AccessAva + AdvicePro + Helplines, all normalised to UT1'
  )
}}

/*
  Stage 2: UNION ALL of topic-level staging models.

  All three sources are normalised to UT1 as SEGMENT before union, which is what
  makes helplines data joinable with the other two sources.

  Grain: one row per conversation/case x topic (topic-mention grain).
  QUERY_COUNT = 1 per row — SUM gives topic mention counts, not query/case counts.
  This is intentional and documented: a conversation touching 3 topics contributes 3.

  Nulls by source:
    AccessAva  — all columns populated where available
    AdvicePro  — HAS_LETTER = 0 always; AGE_BAND and LOCALITY_NAME populated where available
    Helplines  — AGE_BAND NULL, HAS_LETTER NULL, LOCALITY_NAME NULL (pre-aggregated source)
*/

SELECT * FROM {{ ref('stg_accessava_segments') }}
UNION ALL
SELECT * FROM {{ ref('stg_advicepro_segments') }}
UNION ALL
SELECT * FROM {{ ref('stg_helplines') }}
