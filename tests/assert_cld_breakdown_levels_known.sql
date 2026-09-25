-- Every hierarchical column in the CLD RAW models must level to a KNOWN
-- value. 'unrecognised' means a breakdown value appeared that the level
-- macros in macros/cld_breakdown_level.sql have never seen.
--
-- This is the loud-failure half of a deliberate design: the level macros do
-- not pattern-match or guess. A future DHSC release that adds an age band, a
-- support setting or an ethnicity group will fail HERE rather than silently
-- landing in whichever bucket a regex happened to match, which would corrupt
-- every aggregate that pins a level.
--
-- DO NOT fix a failure by widening a pattern. Look at the new value, work out
-- which level it actually sums into, and add it to the macro explicitly.

{% set models = ['stg_cld_assessments', 'stg_cld_long_term_support'] %}

{% for m in models %}
SELECT
    '{{ m }}'            AS model_name,
    'breakdown_level'    AS column_name,
    breakdown_dimension  AS dimension,
    breakdown_value      AS offending_value,
    COUNT(*)             AS row_count
FROM {{ ref(m) }}
WHERE breakdown_level = 'unrecognised'
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    '{{ m }}',
    'area_unit_level',
    'area_unit',
    area_unit,
    COUNT(*)
FROM {{ ref(m) }}
WHERE area_unit_level = 'unrecognised'
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    '{{ m }}',
    'support_setting_level',
    'support_setting',
    support_setting,
    COUNT(*)
FROM {{ ref(m) }}
WHERE support_setting_level = 'unrecognised'
GROUP BY 1, 2, 3, 4

{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
