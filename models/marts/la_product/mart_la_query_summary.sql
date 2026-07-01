{{
  config(
    materialized='table',
    description='Cross-source LA query summary: AdvicePro + AccessAva + Helplines, all normalised to UT1 segment. All-time grain.'
  )
}}

/*
  Mart: LA query summary aggregated across all three source systems.

  Grain: one row per LA x source system x UT1 segment (all time).
  QUERY_COUNT = total topic mentions for that combination.

  All three sources use the UT1-mapped Track 2 staging models:
    - stg_advicepro_segments  -> UT1 via universal_themes_map
    - stg_accessava_segments  -> UT1 via topic_entry_point_map
    - stg_helplines           -> UT1 natively (pre-aggregated)

  QUERY_COUNT = 1 per topic-mention row. A conversation touching 3 topics contributes 3.
  For rolling time windows, add WHERE QUERY_DATE >= DATEADD('month', -N, CURRENT_DATE()).
*/

SELECT
    LA_NAME,
    SOURCE_SYSTEM,
    SEGMENT,
    SUM(QUERY_COUNT) AS QUERY_COUNT

FROM (
    SELECT LA_NAME, SOURCE_SYSTEM, SEGMENT, QUERY_COUNT
    FROM {{ ref('stg_advicepro_segments') }}

    UNION ALL

    SELECT LA_NAME, SOURCE_SYSTEM, SEGMENT, QUERY_COUNT
    FROM {{ ref('stg_accessava_segments') }}

    UNION ALL

    SELECT LA_NAME, SOURCE_SYSTEM, SEGMENT, QUERY_COUNT
    FROM {{ ref('stg_helplines') }}
)

GROUP BY LA_NAME, SOURCE_SYSTEM, SEGMENT
