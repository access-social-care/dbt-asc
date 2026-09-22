{{
  config(
    materialized='table',
    description='CLD assessments as a rate per 100,000 population, monthly, by breakdown'
  )
}}

/*
  Stage: EXTERNAL_DATA.RAW.STG_CLD_ASSESSMENTS / cld_published_population
         -> EXTERNAL_DATA.NORMALISED.

  FLOW measure. A rate here is 'people newly assessed per 100,000 population
  in that month'. Summing monthly rates is NOT meaningful - sum the counts in
  RAW over a contiguous window and divide once, or compare month-on-month.

  Grain out: same as the RAW model it reads, minus the rows that have no
  published denominator (see macros/cld_normalised.sql for the exact list and
  why the join is inner rather than left).

  Rates are computed from monthly counts against a frozen published
  denominator, NOT lifted from the published annual per-100k column. The
  reasoning is in macros/cld_normalised.sql.
*/

{{ cld_normalised(raw_model='stg_cld_assessments') }}
