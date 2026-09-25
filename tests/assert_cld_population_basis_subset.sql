-- The eligible_population denominator basis must be a strict SUBSET of the published
-- basis on (area_code, dimension, dimension_value).
--
-- macros/cld_normalised.sql relies on this: it INNER joins the published
-- basis and LEFT joins eligible_population on top. An eligible_population key with no
-- published counterpart would therefore never appear in NORMALISED at all -
-- the denominator would exist, be correct, and be silently unreachable.
--
-- This is the kind of thing that breaks on a regeneration rather than on a
-- code change: a relabelled ONS ethnic-group category, a boundary revision
-- that adds a UTLA, or a future RM070 pull at finer granularity would each
-- produce eligible_population keys the published basis has never heard of.
-- seeds/generate_cld_population_seed.py asserts the same property at
-- generation time; this is the half that keeps holding afterwards.

WITH eligible_population AS (
    SELECT area_code, dimension, dimension_value
    FROM {{ ref('cld_published_population') }}
    WHERE population_base = 'eligible_population'
),

published AS (
    SELECT area_code, dimension, dimension_value
    FROM {{ ref('cld_published_population') }}
    WHERE population_base = 'published'
)

SELECT
    a.area_code,
    a.dimension,
    a.dimension_value
FROM eligible_population a
LEFT JOIN published p
    ON  a.area_code       = p.area_code
    AND a.dimension       = p.dimension
    AND a.dimension_value = p.dimension_value
WHERE p.area_code IS NULL
