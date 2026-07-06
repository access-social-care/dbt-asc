{{
  config(
    materialized='table',
    description='Combined LA data at topic-mention grain: AccessAva + AdvicePro + Helplines, all normalised to UT1/UT2'
  )
}}

/*
  Stage 2: UNION ALL of topic-level staging models.

  All three sources are normalised to UT1 as SEGMENT before union, which is what
  makes helplines data joinable with the other two sources.

  Grain: one row per conversation/case x topic (topic-mention grain) — NOT one
  row per query/interaction. A conversation touching 3 topics contributes 3 rows.
  QUERY_COUNT = 1 per row — SUM gives topic mention counts, not query/case counts.
  Name reflects this: this is a union of topic mentions, not a union of queries.

  Nulls by source:
    AccessAva  — all columns populated where available
    AdvicePro  — HAS_LETTER = 0 always; AGE_BAND and LOCALITY_NAME populated where available
    Helplines  — UT2 NULL, AGE_BAND NULL, HAS_LETTER NULL, LOCALITY_NAME NULL (pre-aggregated source)
*/

SELECT * FROM {{ ref('stg_accessava') }}
UNION ALL
SELECT * FROM {{ ref('stg_advicepro') }}
UNION ALL
SELECT * FROM {{ ref('stg_helplines') }}
