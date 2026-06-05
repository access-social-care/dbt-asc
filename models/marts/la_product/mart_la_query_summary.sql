{{
  config(
    materialized='table',
    description='Cross-source LA query summary: AdvicePro + AccessAva (by segment) + Helplines'
  )
}}

/*
  Mart: LA query summary aggregated across all three source systems.

  Grain: one row per LA × source system × segment combination.
  QUERY_COUNT = total interactions / calls for that combination (all time).

  Sources:
    - stg_advicepro      → AdvicePro cases. SEGMENT = NULL (no shared taxonomy yet).
    - stg_accessava_segments → AccessAva conversations, SEGMENT = CASE_SPECIFIC_ISSUES_GROUP
                              (one row per segment per conversation after LATERAL FLATTEN).
    - stg_helplines      → Helplines calls. SEGMENT = UT1. QUERY_COUNT = N (pre-aggregated).

  Use QUERY_DATE for time-windowed versions. Add a WHERE clause on QUERY_DATE above
  the GROUP BY to produce rolling period variants (e.g. last 3/6/12 months).

  Note on double-counting: AccessAva conversations with multiple segments contribute
  QUERY_COUNT = 1 per segment row. This is intentional — segment-level counts reflect
  how many interactions touched each topic, not unique conversations. Use stg_la_queries
  (which references stg_accessava, not stg_accessava_segments) for unique conversation counts.
*/

SELECT
    LA_NAME,
    SOURCE_SYSTEM,
    SEGMENT,
    SUM(QUERY_COUNT)   AS QUERY_COUNT

FROM (
    SELECT LA_NAME, SOURCE_SYSTEM, SEGMENT, QUERY_COUNT
    FROM {{ ref('stg_advicepro') }}

    UNION ALL

    SELECT LA_NAME, SOURCE_SYSTEM, SEGMENT, QUERY_COUNT
    FROM {{ ref('stg_accessava_segments') }}

    UNION ALL

    SELECT LA_NAME, SOURCE_SYSTEM, SEGMENT, QUERY_COUNT
    FROM {{ ref('stg_helplines') }}
)

GROUP BY LA_NAME, SOURCE_SYSTEM, SEGMENT
