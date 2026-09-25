{#
  Shared body for the two CLD RAW staging models.

  WHY A MACRO RATHER THAN TWO COPIES
  ----------------------------------
  The previous pair of models were deliberate near-copies, with a comment
  saying the logic was identical "so a fix to one is an obvious fix to both".
  That was reasonable at two models reading one sheet each. It is not
  reasonable now: each model stacks THREE source tables, so the duplication
  would be six near-identical blocks, and the four bugs fixed in these models
  on 2026-09-16 (NULL-propagating suppression check, TRY_TO_DATE on a
  non-date, LA_CODE-as-partition-key, missing database override) were exactly
  the kind that get fixed in one copy and missed in the other.

  The two MODELS stay separate, which is the part that actually matters - a
  consumer must not be able to sum a stock measure alongside a flow one by
  accident.

  WHAT THIS DOES
  --------------
  Stacks Tables 1-3 (age group / gender / ethnicity) of one measure into a
  single long table keyed on breakdown_dimension + breakdown_value, then:
    1. drops the publisher's "Total" metric subtotal row
    2. parses the month header into a real date
    3. types the value, keeping "[c]" suppressed cells NULL rather than 0
    4. resolves overlapping vintages down to the newest per period
    5. derives an explicit level for every hierarchical column

  UNION ALL, not JOIN. Tables 1-3 are the same LA-month grid measured three
  different ways; joining them would produce a cross-product.
#}

{% macro cld_stacked_raw(measure, source_age, source_gender, source_ethnicity, has_support_setting) %}

{%- if has_support_setting -%}
    {%- set support_col = 'support_setting,' -%}
{%- else -%}
    {%- set support_col = 'CAST(NULL AS VARCHAR) AS support_setting,' -%}
{%- endif -%}

WITH stacked AS (

    SELECT
        area, la_code, area_code, area_unit,
        {{ support_col }}
        'age_group'                                   AS breakdown_dimension,
        age_group                                     AS breakdown_value,
        metric, value, _publication_date, _source_url, _run_at
    FROM {{ source('data_portal_landing', source_age) }}

    UNION ALL

    SELECT
        area, la_code, area_code, area_unit,
        {{ support_col }}
        'gender'                                      AS breakdown_dimension,
        gender                                        AS breakdown_value,
        metric, value, _publication_date, _source_url, _run_at
    FROM {{ source('data_portal_landing', source_gender) }}

    UNION ALL

    SELECT
        area, la_code, area_code, area_unit,
        {{ support_col }}
        'ethnicity'                                   AS breakdown_dimension,
        ethnicity                                     AS breakdown_value,
        metric, value, _publication_date, _source_url, _run_at
    FROM {{ source('data_portal_landing', source_ethnicity) }}

),

parsed AS (

    SELECT
        area                                          AS area_name,
        la_code,
        area_code,
        area_unit,
        support_setting,
        breakdown_dimension,
        breakdown_value,
        metric                                        AS metric_raw,
        TRIM(
            IFF(metric ILIKE '%[p]', LEFT(metric, LENGTH(metric) - 3), metric)
        )                                             AS metric_label,
        (metric ILIKE '%[p]')                         AS is_provisional,
        {#- COALESCE, not a bare comparison: TRIM(NULL) = '[c]' evaluates to
            NULL under SQL three-valued logic, not FALSE, so a bare
            comparison silently NULLs the flag on every null-valued row. -#}
        COALESCE(TRIM(value) = '[c]', FALSE)          AS is_suppressed,
        value                                         AS value_raw,
        {#- NOT a real date. The checker tags this dataset with its release
            PERIOD ("2026-03"), not a calendar date, so TRY_TO_DATE() on it
            returns NULL silently and the vintage ranking below would fall
            back entirely to run_at - i.e. to load order, which is an
            execution artifact, not data freshness. Kept as text; "YYYY-MM"
            sorts correctly lexicographically, which is all ranking needs. -#}
        _publication_date                             AS publication_date,
        _source_url                                   AS source_url,
        TRY_TO_TIMESTAMP_NTZ(_run_at)                 AS run_at
    FROM stacked

),

typed AS (

    SELECT
        area_name, la_code, area_code, area_unit, support_setting,
        breakdown_dimension, breakdown_value,
        metric_raw, metric_label, is_provisional, is_suppressed,

        {#- MON takes a 3-letter abbreviation; published headers are full
            month names, hence LEFT(...,3). TRY_ so a bad header lands NULL
            and is caught by tests/assert_cld_metric_parses.sql rather than
            aborting the build.
            Two header shapes: flow measures publish "October 2024", stock
            measures publish the month-END date "31 October 2024". The
            leading day is stripped so both land on the first of the month
            and the two models share one period key. Without the strip every
            long-term support row parsed NULL and was held back (2026-09-25).
            Keep in sync with tests/assert_cld_metric_parses.sql. -#}
        TRY_TO_DATE(
            LEFT(SPLIT_PART(REGEXP_REPLACE(metric_label, '^[0-9]{1,2} ', ''), ' ', 1), 3)
            || ' ' || SPLIT_PART(REGEXP_REPLACE(metric_label, '^[0-9]{1,2} ', ''), ' ', 2)
            || ' 01',
            'MON YYYY DD'
        )                                             AS period_start,

        {#- Suppressed cells stay NULL, never 0. A suppressed count means
            "fewer than 5", and zeroing it understates every aggregate that
            touches it. -#}
        IFF(
            TRIM(value_raw) = '[c]',
            NULL,
            TRY_TO_NUMBER(REPLACE(TRIM(value_raw), ',', ''))
        )                                             AS value_numeric,

        value_raw, publication_date, source_url, run_at
    FROM parsed

    {#- Total exclusion. A "Total" row is the sum of that LA's own months,
        not an extra category: verified end-to-end 2026-09-14 (a real LA's
        12 monthly values summed to exactly its Total row). Leaving it in
        doubles every 12-month sum. #}
    WHERE UPPER(metric_label) NOT LIKE 'TOTAL%'

),

ranked AS (

    {#- Partitioned on AREA_CODE, never LA_CODE. LA_CODE is blank for England,
        for regions, and for the 2023 reorganisation unitaries; NULLs group as
        equal in a window PARTITION BY, so partitioning on it would merge every
        blank-LA_CODE area into one partition and drop all but one of them at
        the vintage_rank = 1 filter - silently, with no error. AREA_CODE is
        populated on every row.

        breakdown_dimension AND breakdown_value are both in the key. The
        dimension is not redundant: the literal 'All' occurs in all three
        dimensions, so without it the age, gender and ethnicity "All" rows
        for one LA-month would collide and two of the three would be dropped.

        area_unit is in the key because ADASS Region and Region are
        overlapping geographies that can share an area_code. #}
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY area_code, area_unit, support_setting,
                         breakdown_dimension, breakdown_value, period_start
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
    {{ cld_area_unit_level('area_unit') }}             AS area_unit_level,
    support_setting,
    {{ cld_support_setting_level('support_setting') }} AS support_setting_level,
    breakdown_dimension,
    breakdown_value,
    CASE breakdown_dimension
        WHEN 'age_group' THEN {{ cld_age_group_level('breakdown_value') }}
        WHEN 'gender'    THEN {{ cld_gender_level('breakdown_value') }}
        WHEN 'ethnicity' THEN {{ cld_ethnicity_level('breakdown_value') }}
        ELSE 'unrecognised'
    END                                                AS breakdown_level,
    '{{ measure }}'                                    AS measure,
    period_start,
    LAST_DAY(period_start)                             AS period_end,
    value_numeric                                      AS value,
    is_suppressed,
    is_provisional,
    metric_raw,
    publication_date,
    source_url,
    run_at
FROM ranked
WHERE vintage_rank = 1

{% endmacro %}
