{{ config(severity='warn') }}

-- Reconciliation, not a hard gate - hence severity=warn.
--
-- We compute rates ourselves from monthly counts rather than lifting the
-- published annual per-100k column (see macros/cld_normalised.sql for why).
-- That means nothing independently confirms our arithmetic. This compares our
-- computed England figures against the denominators we seeded from the
-- publisher: for the all-ages, all-settings, Local-Authority-level rows, the
-- population we divide by must be the published national total.
--
-- It warns rather than fails because the two are not expected to match to the
-- unit: published counts are rounded to the nearest 5, and our monthly series
-- covers a different window than the publisher's rolling 12 months.

SELECT
    n.measure,
    n.period_start,
    n.population_published,
    s.population AS seeded_population,
    n.rate_per_100k_published
FROM {{ ref('norm_cld_assessments') }} n
JOIN {{ ref('cld_published_population') }} s
    ON  s.area_code        = n.area_code
    AND s.dimension        = n.breakdown_dimension
    AND s.dimension_value  = n.breakdown_value
    AND s.population_base  = 'published'
WHERE n.area_code = 'E92000001'
  AND n.breakdown_dimension = 'age_group'
  AND n.breakdown_value = 'All'
  AND n.population_published <> s.population
