{{
  config(
    materialized='table',
    description='CLD quarterly assessments (flow measure), all three monthly breakdowns stacked, typed and deduped to the latest vintage per area-period'
  )
}}

/*
  Stage: EXTERNAL_DATA.LANDING.CLD_ASSESSMENTS{,_GENDER,_ETHNICITY}
         -> typed, deduped, long-by-breakdown.

  Grain out: one row per
    (area_code, area_unit, breakdown_dimension, breakdown_value, period_start).

  MEASURE SEMANTICS: assessments is a FLOW measure - people assessed in that
  month who had NOT had long-term support in the prior 12 months. A sum over a
  contiguous 12-month window IS valid here, unlike the sibling stock measure
  in stg_cld_long_term_support. That validity depends on two things having
  happened first: the publisher's "Total" metric rows being excluded (done in
  the macro), and the query filtering to a SINGLE breakdown_level. Summing
  across levels multiplies the figure - see the header of
  macros/cld_breakdown_level.sql.

  SHAPE vs stg_cld_long_term_support: this file has no support_setting
  dimension, so support_setting and support_setting_level are NULL on every
  row. The column is retained so both models present the same shape to
  consumers. The two models are kept separate, rather than unioned into one,
  so a consumer cannot accidentally sum a flow measure alongside a stock one.

  All shared transformation logic lives in macros/cld_stacked_raw.sql.
*/

{{ cld_stacked_raw(
    measure='assessments',
    source_age='cld_assessments',
    source_gender='cld_assessments_gender',
    source_ethnicity='cld_assessments_ethnicity',
    has_support_setting=false
) }}
