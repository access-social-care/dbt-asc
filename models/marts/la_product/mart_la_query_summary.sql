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

  Reads from stg_la_topic_mentions, which already unions all three sources at
  UT1-mapped topic-mention grain. This mart collapses that to LA x source x segment.

  QUERY_COUNT = 1 per topic-mention row before aggregation. A conversation
  touching 3 topics contributes 3. For rolling time windows, add
  WHERE QUERY_DATE >= DATEADD('month', -N, CURRENT_DATE()).
*/

SELECT
    LA_NAME,
    SOURCE_SYSTEM,
    SEGMENT,
    SUM(QUERY_COUNT) AS QUERY_COUNT

FROM {{ ref('stg_la_topic_mentions') }}

GROUP BY LA_NAME, SOURCE_SYSTEM, SEGMENT
