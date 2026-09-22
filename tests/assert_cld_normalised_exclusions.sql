-- Pins the set of RAW rows that NORMALISED legitimately drops.
--
-- macros/cld_normalised.sql joins RAW to the published-population seed with an
-- INNER join, because some breakdown values have no published denominator and
-- never will. That is correct, but it makes the NORMALISED models silently
-- smaller than RAW - and silent shrinkage is exactly how a real coverage
-- regression (a renamed dimension value, a seed that failed to load, a join
-- key that changed shape) would hide.
--
-- So: the KNOWN unmatched values are listed here. Anything unmatched that is
-- NOT on the list fails the test. If DHSC renames "No data: Refused", this
-- goes red on the rename rather than on the day someone notices the rates
-- look thin.
--
-- Verified against the sep2025 backfill, 2026-09-22.

{% set known_unmatched = [
    '85 to 94',
    '95 and above',
    'Unknown',
    'Other',
    'No data',
    'No data: Refused',
    'No data: Undeclared or not known'
] %}

{% set pairs = [
    ('stg_cld_assessments', 'norm_cld_assessments'),
    ('stg_cld_long_term_support', 'norm_cld_long_term_support')
] %}

{% for raw_model, norm_model in pairs %}
SELECT
    '{{ raw_model }}'      AS model_name,
    r.breakdown_dimension,
    r.breakdown_value      AS unmatched_value,
    r.area_unit,
    COUNT(*)               AS row_count
FROM {{ ref(raw_model) }} r
LEFT JOIN {{ ref('cld_published_population') }} p
    ON  r.area_code           = p.area_code
    AND r.breakdown_dimension = p.dimension
    AND r.breakdown_value     = p.dimension_value
WHERE r.value IS NOT NULL
  AND p.area_code IS NULL
  -- Known, permanent gaps: no published ADASS-region population at all...
  AND r.area_unit <> 'ADASS Region'
  -- ...and these breakdown values have no published denominator.
  AND r.breakdown_value NOT IN (
      {% for v in known_unmatched %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %}
  )
GROUP BY 1, 2, 3, 4

{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
