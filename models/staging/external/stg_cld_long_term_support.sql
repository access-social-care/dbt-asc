{{
  config(
    materialized='table',
    description='CLD quarterly long-term support (stock measure), all three monthly breakdowns stacked, typed and deduped to the latest vintage per area-period'
  )
}}

/*
  Stage: EXTERNAL_DATA.LANDING.CLD_LONG_TERM_SUPPORT{,_GENDER,_ETHNICITY}
         -> typed, deduped, long-by-breakdown.

  Grain out: one row per
    (area_code, area_unit, support_setting, breakdown_dimension,
     breakdown_value, period_start).

  MEASURE SEMANTICS: long-term support is a STOCK measure - a point-in-time
  snapshot of people receiving LA-arranged support at each month-end. NEVER
  sum it across months; the correct aggregations are the latest snapshot or a
  mean over month-ends. England rose 665,925 -> 684,920 across a single
  published window, so a mean is also fragile on a trending series - carry
  the end-of-period snapshot alongside it if the trend matters.

  TWO CROSSED HIERARCHIES here, not one: support_setting (All = Community +
  Nursing care + Residential care + Prison) and whichever breakdown_dimension
  the row belongs to. Both carry an "All" subtotal row. A query must pin a
  level on BOTH or it multiplies the figure. support_setting is in the dedupe
  partition key for the same reason.

  All shared transformation logic lives in macros/cld_stacked_raw.sql.
*/

{{ cld_stacked_raw(
    measure='long_term_support',
    source_age='cld_long_term_support',
    source_gender='cld_long_term_support_gender',
    source_ethnicity='cld_long_term_support_ethnicity',
    has_support_setting=true
) }}
