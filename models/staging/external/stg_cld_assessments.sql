{{
  config(
    materialized='table',
    description='CLD quarterly assessments (flow measure), typed and deduped to the latest vintage per LA-month'
  )
}}

/*
  Stage: EXTERNAL_DATA.LANDING.CLD_ASSESSMENTS -> typed, deduped LA-month grain.

  !! UNVERIFIED (2026-09-16) !! Written without any Snowflake connection. This
  model has never been compiled or run. See README.md in this folder for the
  exact commands needed to validate it before it is trusted.

  Grain out: one row per (LA_CODE, AGE_GROUP, PERIOD_START).

  MEASURE SEMANTICS: assessments is a FLOW measure - people assessed in that
  month who had NOT had long-term support in the prior 12 months. A sum over a
  contiguous 12-month window IS valid here, unlike the sibling stock measure
  in stg_cld_long_term_support. That validity depends entirely on the "Total"
  rows having been excluded first (see below); summing with them still in
  double-counts every LA.

  SHAPE DIFFERENCE vs stg_cld_long_term_support: this file has one fewer
  breakdown dimension - there is no `support_setting` column at all (confirmed
  from the checker's registry entry: tidy.id_columns omits it). Otherwise the
  tidy shape, the "[p]" provisional suffix, the "[c]" suppression marker and
  the trailing "Total" metric row behave identically. The two models are kept
  separate rather than unioned so a consumer cannot accidentally sum a stock
  measure alongside a flow one.

  The four transformations (Total exclusion, month parsing, value typing +
  suppression, latest-vintage-per-period dedupe) are documented in full in
  stg_cld_long_term_support.sql and are not repeated here; the logic is
  deliberately identical so a fix to one is an obvious fix to both.
*/

WITH landing AS (

    SELECT
        area,
        age_group,
        la_code,
        area_code,
        area_unit,
        metric,
        value,
        _publication_date,
        _source_url,
        _run_at
    FROM {{ source('data_portal_landing', 'cld_assessments') }}

),

parsed AS (

    SELECT
        area                                          AS area_name,
        age_group,
        la_code,
        area_code,
        area_unit,
        metric                                        AS metric_raw,
        TRIM(
            IFF(metric ILIKE '%[p]', LEFT(metric, LENGTH(metric) - 3), metric)
        )                                             AS metric_label,
        (metric ILIKE '%[p]')                         AS is_provisional,
        -- COALESCE, not a bare comparison: TRIM(NULL) = '[c]' evaluates to
        -- NULL (SQL three-valued logic), not FALSE - see
        -- stg_cld_long_term_support.sql for the full note.
        COALESCE(TRIM(value) = '[c]', FALSE)          AS is_suppressed,
        value                                         AS value_raw,
        TRY_TO_DATE(_publication_date)                AS publication_date,
        _source_url                                   AS source_url,
        TRY_TO_TIMESTAMP_NTZ(_run_at)                 AS run_at
    FROM landing

),

typed AS (

    SELECT
        area_name,
        age_group,
        la_code,
        area_code,
        area_unit,
        metric_raw,
        metric_label,
        is_provisional,
        is_suppressed,

        -- MON takes a 3-letter abbreviation; published headers are full month
        -- names, hence the LEFT(...,3). TRY_ so a bad header lands as NULL and
        -- is caught by tests/assert_cld_metric_parses.sql, not as a cast error.
        TRY_TO_DATE(
            LEFT(SPLIT_PART(metric_label, ' ', 1), 3)
            || ' ' || SPLIT_PART(metric_label, ' ', 2)
            || ' 01',
            'MON YYYY DD'
        )                                             AS period_start,

        -- Suppressed cells stay NULL, never 0.
        IFF(
            TRIM(value_raw) = '[c]',
            NULL,
            TRY_TO_NUMBER(REPLACE(TRIM(value_raw), ',', ''))
        )                                             AS value_numeric,

        value_raw,
        publication_date,
        source_url,
        run_at
    FROM parsed

    -- Total exclusion. A "Total" row is the sum of the same LA's own months,
    -- not an extra category: verified end-to-end 2026-09-14 (a real LA's 12
    -- month values summed to exactly its Total row). Leaving it in doubles
    -- every 12-month sum, which is the headline aggregation for this measure.
    WHERE UPPER(metric_label) NOT LIKE 'TOTAL%'

),

ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY la_code, age_group, period_start
            ORDER BY publication_date DESC NULLS LAST,
                     run_at DESC NULLS LAST
        ) AS vintage_rank
    FROM typed
    WHERE period_start IS NOT NULL

)

SELECT
    la_code,
    area_code,
    area_name,
    area_unit,
    age_group,
    'assessments'                          AS measure,
    period_start,
    LAST_DAY(period_start)                 AS period_end,
    value_numeric                          AS value,
    is_suppressed,
    is_provisional,
    metric_raw,
    publication_date,
    source_url,
    run_at
FROM ranked
WHERE vintage_rank = 1
