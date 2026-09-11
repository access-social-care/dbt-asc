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

  KNOWN EXTERNAL CONSUMER (2026-09): the helplines_data repo's State of the
  Nation (SOTN) build reads this model's output directly via a Snowflake
  grant on ANALYTICS.STAGING_ACS_HELPLINES (ROLE_ETL_WRITE - see
  admin/snowflake_helplines_advicepro_accessava_grant.sql), combining it with
  its own PARTNER_HISTORY_MENCAP/PARTNER_HISTORY_RNIB tables into
  HELPLINES.PUBLIC.HELPLINES_STATE_OF_THE_NATION. That's a cross-repo,
  cross-database read outside dbt's own ref()/lineage graph, so `dbt` will
  not flag it as a breaking change. Do not restructure this model's grain,
  column names, or SOURCE_SYSTEM/UT1/UT2 semantics without checking with the
  helplines_data pipeline owner first.
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
