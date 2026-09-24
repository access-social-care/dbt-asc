{#
  Shared body for the two CLD NORMALISED models: a RAW count expressed as a
  rate per 100,000, on TWO different denominator bases.

  THE TWO BASES ANSWER DIFFERENT QUESTIONS
  ----------------------------------------
    rate_per_100k_published
        Denominator: the whole population of that group, as published by
        DHSC. "Per head of everyone."

    rate_per_100k_asc_eligible
        Denominator: people who could plausibly need adult social care -
        everyone 65+, plus working-age people recorded disabled under the
        Equality Act (ONS Census 2021, RM070 custom cross-tab). "Of the
        people who might need care, what share are getting it."

  The second is an ACCESS rate and the first is a utilisation share. They
  diverge most exactly where it matters: nationally, White residents are
  90.6% of the ASC-eligible population but only 83.3% of the general
  population, because the ASC-eligible group is older. A whole-population
  denominator therefore flatters minority-group access rates. Carrying both
  makes that visible rather than forcing a choice here.

  WIDE, NOT LONG - AND DELIBERATELY SO
  ------------------------------------
  Both bases sit on ONE row as separate columns, rather than the basis being
  part of the grain. If basis were a grain column, every consumer who forgot
  to filter on it would silently double-count - the same trap the hierarchy
  level columns exist to prevent, and not one worth introducing twice. This
  way the grain of NORMALISED is exactly the grain of RAW (minus undenominated
  rows), and picking a basis is choosing a column, which is hard to do by
  accident.

  asc_eligible IS SPARSE. It is NULL wherever RM070 cannot reach:
    - every gender row            (RM070 has no sex dimension)
    - fine age bands              (RM070's age is 16-64 / 65+ only)
    - ethnicity sub-groups        (RM070 has 6 top-level groups only)
    - every Region row            (needs a region-to-LA membership list that
                                   neither source provides)
  A NULL there means "this basis does not reach this row", never "zero".

  THE PUBLISHED JOIN IS INNER, THE ASC_ELIGIBLE JOIN IS LEFT
  ----------------------------------------------------------
  Some RAW rows have no published denominator either, and never will:

    age_group   '85 to 94', '95 and above'   (monthly bands are finer than
                                              the annual ones, which stop at
                                              '85 and above')
    age_group   'Unknown'
    gender      'Other', 'Unknown'
    ethnicity   'No data', 'No data: Refused',
                'No data: Undeclared or not known'
    area_unit   every 'ADASS Region' row      (no published ADASS population)

  Those are recording categories or overlapping geographies, not populations -
  there is nothing to divide by under any source. An inner join says they are
  out of scope for a rate; a LEFT join would carry them with a NULL rate and
  invite someone to COALESCE it to zero. The dropped set is pinned by
  tests/assert_cld_normalised_exclusions.sql so a NEW unmatched value - a real
  coverage regression - fails loudly instead of quietly shrinking this table.

  RATE BASE WARNING, published basis only: ethnicity denominators use a
  Census 2021 base (England 44,715,447) while age and gender use the ONS
  mid-year estimate (46,437,085). Published ethnicity rates are therefore not
  directly comparable with published age or gender rates.
  published_population_source carries which applies. The asc_eligible basis
  is Census 2021 throughout, so it has no such split.
#}

{% macro cld_normalised(raw_model) %}

WITH raw_counts AS (

    SELECT *
    FROM {{ ref(raw_model) }}
    {#- Suppressed cells are NULL, and a rate from an unknown numerator is
        meaningless rather than zero. Excluded rather than carried as NULL,
        for the same reason the published join is inner. -#}
    WHERE value IS NOT NULL

),

pop_published AS (

    SELECT
        area_code,
        dimension,
        dimension_value,
        population,
        CASE
            WHEN dimension = 'ethnicity' THEN 'census_2021'
            ELSE 'ons_mid_year_estimate'
        END AS population_source
    FROM {{ ref('cld_published_population') }}
    WHERE population_base = 'published'
      AND population > 0

),

pop_asc_eligible AS (

    SELECT
        area_code,
        dimension,
        dimension_value,
        population
    FROM {{ ref('cld_published_population') }}
    WHERE population_base = 'asc_eligible'
      AND population > 0

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

    r.value                                   AS count_value,

    {#- Basis 1: whole population, as published by DHSC. -#}
    pub.population                            AS population_published,
    pub.population_source                     AS published_population_source,
    ROUND(
        {#- Cast before dividing: both operands are integers and Snowflake
            would otherwise do integer division on the intermediate. -#}
        (r.value::FLOAT / pub.population::FLOAT) * 100000,
        2
    )                                         AS rate_per_100k_published,

    {#- Basis 2: ASC-eligible population (Census 2021 RM070). NULL wherever
        RM070 cannot reach - see the header. NULL means "not reachable on
        this basis", never zero. -#}
    asc_pop.population                        AS population_asc_eligible,
    CASE
        WHEN asc_pop.population IS NULL THEN NULL
        ELSE ROUND(
            (r.value::FLOAT / asc_pop.population::FLOAT) * 100000,
            2
        )
    END                                       AS rate_per_100k_asc_eligible,

    r.is_provisional,
    r.publication_date,
    r.source_url,
    r.run_at

FROM raw_counts r

INNER JOIN pop_published pub
    ON  r.area_code           = pub.area_code
    AND r.breakdown_dimension = pub.dimension
    AND r.breakdown_value     = pub.dimension_value

LEFT JOIN pop_asc_eligible asc_pop
    ON  r.area_code           = asc_pop.area_code
    AND r.breakdown_dimension = asc_pop.dimension
    AND r.breakdown_value     = asc_pop.dimension_value

{% endmacro %}
