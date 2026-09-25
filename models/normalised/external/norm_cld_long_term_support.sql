{{
  config(
    materialized='table',
    description='CLD long_term_support as a rate per 100,000 population, monthly, by breakdown'
  )
}}

/*
  Stage: EXTERNAL_DATA.RAW.STG_CLD_LONG_TERM_SUPPORT / cld_published_population
         -> EXTERNAL_DATA.NORMALISED.

  STOCK measure. A rate here is 'people receiving long-term support per
  100,000 population as at that month-end'. Never sum across months; compare
  snapshots or take a mean over month-ends.

  Grain out: same as the RAW model it reads, minus the rows that have no
  published denominator (see macros/cld_normalised.sql for the exact list and
  why the join is inner rather than left).

  Rates are computed from monthly counts against a frozen published
  denominator, NOT lifted from the published annual per-100k column. The
  reasoning is in macros/cld_normalised.sql.
*/

{{ cld_normalised(raw_model='stg_cld_long_term_support') }}
