{#
  Shared body for the two CLD NORMALISED models: a RAW count divided by the
  matching published population, giving a rate per 100,000.

  WHY WE COMPUTE THIS RATHER THAN LIFT THE PUBLISHED RATE
  -------------------------------------------------------
  Tables 4-6 of each CLD file already carry a "Number of people per 100,000"
  column. We do not use it, for two reasons established 2026-09-22:

    1. It is ANNUAL. One rolling 12-month figure per release, so three data
       points per area across the whole series. Dividing the monthly RAW
       counts instead gives a monthly time series, which is what signal
       detection actually needs.
    2. Its denominator is FROZEN anyway. The Population column is
       byte-identical across the 2025-09, 2025-12 and 2026-03 releases
       (England 46,437,085 in all three). So nothing is lost by lifting that
       denominator once into a seed and doing the division ourselves.

  The published rate remains useful as a reconciliation check, which is what
  tests/warn_cld_normalised_reconciles.sql does.

  THE JOIN IS DELIBERATELY INNER
  ------------------------------
  Some RAW rows legitimately have no denominator and never will:

    age_group   '85 to 94', '95 and above'  (monthly bands are finer than the
                                             annual ones, which stop at
                                             '85 and above')
    age_group   'Unknown'
    gender      'Other', 'Unknown'
    ethnicity   'No data', 'No data: Refused',
                'No data: Undeclared or not known'
    area_unit   every 'ADASS Region' row     (no published ADASS population)

  A LEFT JOIN would carry these through with a NULL rate, which reads as "we
  tried and got nothing" and invites someone to COALESCE it to zero. An INNER
  join says they are out of scope for a rate. The set of dropped values is
  pinned by tests/assert_cld_normalised_exclusions.sql, so a NEW unmatched
  value - a genuine coverage regression - fails loudly instead of quietly
  shrinking this table.

  RATE BASE WARNING: the published ethnicity denominators use a Census 2021
  base (44,715,447 for England) while age and gender use the mid-year estimate
  (46,437,085). Ethnicity rates are therefore NOT directly comparable with age
  or gender rates. population_base carries which one applies to each row.
#}

{% macro cld_normalised(raw_model) %}

WITH raw_counts AS (

    SELECT *
    FROM {{ ref(raw_model) }}
    {#- Suppressed cells are NULL, and a rate computed from an unknown
        numerator is meaningless rather than zero. Excluded here rather than
        carried as NULL for the same reason the join is inner. -#}
    WHERE value IS NOT NULL

),

population AS (

    SELECT
        area_code,
        dimension,
        dimension_value,
        population,
        {#- Which national total this denominator is drawn from. Ethnicity
            populations come from Census 2021, age and gender from the ONS
            mid-year estimate - see the source file's Notes sheet. Carried
            per row so a consumer comparing an ethnicity rate against an age
            rate can see that they do not share a base. -#}
        CASE
            WHEN dimension = 'ethnicity' THEN 'census_2021'
            ELSE 'ons_mid_year_estimate'
        END AS population_base
    FROM {{ ref('cld_published_population') }}
    WHERE population > 0

)

SELECT
    r.la_code,
    r.area_code,
    r.area_name,
    r.area_unit,
    r.area_unit_level,
    r.support_setting,
    r.support_setting_level,
    r.breakdown_dimension,
    r.breakdown_value,
    r.breakdown_level,
    r.measure,
    r.period_start,
    r.period_end,
    r.value                                           AS count_value,
    p.population,
    p.population_base,
    {#- Cast before dividing: both operands are integers and Snowflake would
        otherwise do integer division on the intermediate. -#}
    ROUND(
        (r.value::FLOAT / p.population::FLOAT) * 100000,
        2
    )                                                 AS rate_per_100k,
    r.is_provisional,
    r.publication_date,
    r.source_url,
    r.run_at
FROM raw_counts r
INNER JOIN population p
    ON  r.area_code          = p.area_code
    AND r.breakdown_dimension = p.dimension
    AND r.breakdown_value     = p.dimension_value

{% endmacro %}
