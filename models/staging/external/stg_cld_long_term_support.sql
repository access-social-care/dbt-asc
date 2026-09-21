{{
  config(
    materialized='table',
    description='CLD quarterly long-term support (stock measure), typed and deduped to the latest vintage per LA-month'
  )
}}

/*
  Stage: EXTERNAL_DATA.LANDING.CLD_LONG_TERM_SUPPORT -> typed, deduped LA-month grain.

  !! UNVERIFIED (2026-09-16) !! Written without any Snowflake connection. This
  model has never been compiled or run. See README.md in this folder for the
  exact commands needed to validate it before it is trusted.

  Grain out: one row per (LA_CODE, SUPPORT_SETTING, AGE_GROUP, PERIOD_START).

  MEASURE SEMANTICS: long-term support is a STOCK measure - a point-in-time
  snapshot of people receiving support at each month-end. Correct aggregation
  is the latest snapshot, or a mean across calendar months. NEVER sum it.
  (The sibling model stg_cld_assessments is a FLOW measure, where a sum over a
  contiguous window IS valid. They are separate models for exactly this
  reason - a single combined table invites a consumer to sum a stock measure.)

  Four things this model does, none of which happen upstream:

  1. Drops the "Total" metric rows. The source ODS is wide - one column per
     month PLUS a trailing "Total [p]" column - and the checker's melt turns
     every column into a metric row, so "Total [p]" arrives as a row like any
     month. It is NOT an extra category: it is the sum of the same LA's own
     month values. Verified end-to-end on 2026-09-14 against a real LA - its
     12 monthly values summed to exactly its own Total row. Leaving these in
     doubles every aggregate. This exact bug was found live in
     signal_processing's R code, which is why the exclusion is explicit and
     tested here (tests/assert_no_total_metric_in_cld.sql) rather than assumed.

  2. Parses the raw month header text into real dates. `metric` arrives as the
     published column header, e.g. "July 2025 [p]". The "[p]" suffix means
     provisional and is lifted into its own boolean rather than left glued to
     the period label.

  3. Types `value` and separates suppression from absence. "[c]" is the
     published statistical-disclosure suppression marker. VALUE stays NULL for
     those rows (NOT zero - a suppressed small number is unknown, not none)
     and IS_SUPPRESSED carries the distinction so a consumer can tell a
     suppressed cell apart from a genuinely missing row.

  4. Dedupes to the latest vintage PER PERIOD, not per file. Each quarterly
     release republishes a rolling window of months, and a month present in an
     older vintage can be absent from a newer one. Partitioning by LA-month
     (not by vintage) means an LA-month that only ever appeared in an older
     release survives, while an LA-month covered by several vintages takes the
     newest published value.
*/

WITH landing AS (

    SELECT
        area,
        support_setting,
        age_group,
        la_code,
        area_code,
        area_unit,
        metric,
        value,
        _publication_date,
        _source_url,
        _run_at
    FROM {{ source('data_portal_landing', 'cld_long_term_support') }}

),

parsed AS (

    SELECT
        area                                          AS area_name,
        support_setting,
        age_group,
        la_code,
        area_code,
        area_unit,
        metric                                        AS metric_raw,

        -- "July 2025 [p]" -> "July 2025". Deliberately a suffix trim rather
        -- than a regex: the backslash escaping needed for '\[p\]$' has to
        -- survive both the dbt file and the Snowflake string literal, and
        -- getting that wrong fails silently (the marker just never matches).
        TRIM(
            IFF(metric ILIKE '%[p]', LEFT(metric, LENGTH(metric) - 3), metric)
        )                                             AS metric_label,

        -- Provisional marker, lifted out of the period label.
        (metric ILIKE '%[p]')                         AS is_provisional,

        -- Suppression marker. Checked on the raw cell before any numeric cast,
        -- because TRY_TO_NUMBER('[c]') and TRY_TO_NUMBER(NULL) are both NULL
        -- and this is the only place the two can still be told apart.
        -- COALESCE, not a bare comparison: TRIM(NULL) = '[c]' evaluates to
        -- NULL (SQL three-valued logic), not FALSE, so a genuinely blank
        -- cell - a different data-quality situation to an explicit "[c]"
        -- suppression marker - would otherwise fail the not_null test on
        -- this column instead of correctly reading as "not suppressed".
        COALESCE(TRIM(value) = '[c]', FALSE)          AS is_suppressed,

        value                                         AS value_raw,
        -- NOT a real date - see the full note in stg_cld_assessments.sql.
        -- The checker tags this dataset with its release period ("2026-03"),
        -- not a calendar date, so TRY_TO_DATE() silently nulled it out and
        -- the vintage ranking below was accidentally running on run_at
        -- alone. Kept and ranked as text.
        _publication_date                              AS publication_date,
        _source_url                                   AS source_url,
        TRY_TO_TIMESTAMP_NTZ(_run_at)                 AS run_at
    FROM landing

),

typed AS (

    SELECT
        area_name,
        support_setting,
        age_group,
        la_code,
        area_code,
        area_unit,
        metric_raw,
        metric_label,
        is_provisional,
        is_suppressed,

        -- Month headers are full month names ("July 2025"). Snowflake's MON
        -- format element takes the 3-letter abbreviation, so the month name is
        -- truncated to 3 characters rather than relying on a full-month-name
        -- format element. TRY_ (not TO_) so an unexpected header shape yields
        -- NULL and is caught by the assertion below instead of failing the run
        -- with an opaque cast error.
        TRY_TO_DATE(
            LEFT(SPLIT_PART(metric_label, ' ', 1), 3)
            || ' ' || SPLIT_PART(metric_label, ' ', 2)
            || ' 01',
            'MON YYYY DD'
        )                                             AS period_start,

        -- Suppressed cells stay NULL, never 0. Thousands separators are
        -- stripped because the published cells carry them.
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

    -- (1) Total exclusion. Matched on the de-bracketed label so it catches
    -- "Total", "Total [p]" and any future "Total (revised)"-style variant.
    -- Excluded here rather than filtered downstream: every consumer would
    -- otherwise have to remember, and one that forgets double-counts silently.
    WHERE UPPER(metric_label) NOT LIKE 'TOTAL%'

),

ranked AS (

    SELECT
        *,
        -- (4) Latest vintage per LA-month. SUPPORT_SETTING and AGE_GROUP are
        -- in the partition key even though the checker's row_filter currently
        -- pins both to "All": if that filter is ever widened, this keeps each
        -- breakdown as its own series instead of silently collapsing them.
        -- Partitioned by AREA_CODE, not LA_CODE - LA_CODE is blank for some
        -- legitimate LAs (confirmed live: 2023 reorganisation unitaries),
        -- and NULL groups as equal in a window PARTITION BY, which would
        -- silently merge every blank-LA_CODE authority into one partition
        -- and drop all but one of their rows for any shared period.
        ROW_NUMBER() OVER (
            PARTITION BY area_code, support_setting, age_group, period_start
            ORDER BY publication_date DESC NULLS LAST,
                     run_at DESC NULLS LAST
        ) AS vintage_rank
    FROM typed
    -- Rows whose month header could not be parsed are held out rather than
    -- carried through with a NULL period. tests/assert_cld_metric_parses.sql
    -- fails if any such row exists in the landing table, so this filter can
    -- never quietly swallow a real month.
    WHERE period_start IS NOT NULL

)

SELECT
    la_code,
    area_code,
    area_name,
    area_unit,
    support_setting,
    age_group,
    'long_term_support'                    AS measure,
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
