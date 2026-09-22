-- Guards the grain contract of the CLD RAW models after the checker's
-- row_filter was removed (2026-09-22).
--
-- Before that change the checker pinned Age group = All, Support setting =
-- All and Area unit = Local Authority, so the models could only ever contain
-- one level. They now contain every level the publisher ships, which is the
-- point - but it means a naive SUM over one of these models double- or
-- triple-counts, and NOTHING in the existing test suite would notice: the
-- uniqueness test stays green because subtotal rows are genuinely distinct
-- rows.
--
-- This test does not forbid subtotal rows (they are legitimate data). It
-- asserts that the LEVEL COLUMNS that make them distinguishable are always
-- populated, so a consumer always has a way to pin one level. A NULL level on
-- a non-NULL dimension means the level derivation silently missed a row and
-- the subtotals have become indistinguishable from the categories.

{% set models = ['stg_cld_assessments', 'stg_cld_long_term_support'] %}

{% for m in models %}
SELECT
    '{{ m }}'        AS model_name,
    'breakdown'      AS which,
    breakdown_dimension,
    breakdown_value  AS offending_value,
    COUNT(*)         AS row_count
FROM {{ ref(m) }}
WHERE breakdown_value IS NOT NULL
  AND breakdown_level IS NULL
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    '{{ m }}',
    'area_unit',
    'area_unit',
    area_unit,
    COUNT(*)
FROM {{ ref(m) }}
WHERE area_unit IS NOT NULL
  AND area_unit_level IS NULL
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    '{{ m }}',
    'support_setting',
    'support_setting',
    support_setting,
    COUNT(*)
FROM {{ ref(m) }}
WHERE support_setting IS NOT NULL
  AND support_setting_level IS NULL
GROUP BY 1, 2, 3, 4

{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
