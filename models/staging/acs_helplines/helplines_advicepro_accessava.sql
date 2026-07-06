{{
  config(
    materialized='table',
    schema='staging_acs_helplines',
    description='Cross-source query counts aggregated to UT1/UT2 - Helplines + AdvicePro + AccessAva'
  )
}}

/*
  Cross-source aggregate at UNIVERSAL THEME (UT1/UT2) grain, monthly.

  Grain: LA_NAME x MONTH_DATE x SOURCE_SYSTEM x UT1 x UT2.

  Reads from stg_la_topic_mentions (topic-mention grain, all three sources already
  UT1/UT2-mapped) and collapses to monthly grain. This used to duplicate the UT1/UT2
  mapping logic directly against raw sources — rebuilt to read from the shared
  staging models so a taxonomy fix only needs to happen in one place.

  QUERY_COUNT = SUM of topic mentions for that LA x month x source x UT1 x UT2
  combination. For Helplines this is already pre-aggregated upstream; for
  AccessAva/AdvicePro it is topic-mention count (a conversation/case touching
  N topics contributes N).

  'Unmapped' = the source value exists but has no row in its UT1 map (taxonomy
  drift or map gap). Kept visible rather than dropped - tests/warn_unmapped_ut1_share.sql
  warns when any source's Unmapped share exceeds threshold.
  'Unmatched' = AccessAva topic_entry_point was NULL (no topic recorded at all).
*/

SELECT
    LA_NAME,
    DATE_TRUNC('month', QUERY_DATE)::DATE AS MONTH_DATE,
    SOURCE_SYSTEM,
    SEGMENT                               AS UT1,
    UT2,
    SUM(QUERY_COUNT)                      AS QUERY_COUNT

FROM {{ ref('stg_la_topic_mentions') }}

GROUP BY LA_NAME, DATE_TRUNC('month', QUERY_DATE)::DATE, SOURCE_SYSTEM, SEGMENT, UT2
