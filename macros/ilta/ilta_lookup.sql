{#
  ILTA label lookup for one source system. Returns a query body with columns
  (feature, key, ilta) - one row per feature x lower(trim(raw value)).

  Precedence (first wins): ilta_map_supplement seed > REFERENCE.PUBLIC.ILTA_MAP_*
  table > the raw value already being an ILTA label (identity, e.g. 'Female').
  A mapped label is only kept if it exists in the ilta_categories seed for that
  feature, so a corrupted map row (e.g. a shifted CSV column producing
  'e.g. dyslexia') falls through to 'Unmapped' in the model instead of leaking
  into the report as a fake category.

  Feature names are normalised: the two map tables name the same field
  differently (disability_text vs disability, disability_n_y vs
  'Do you have a disability').

  Args:
    source_system  - value in ilta_map_supplement.source_system
    map_source     - name of the source() table in the 'reference' source
    raw_col        - the raw-value column in that map (ACCESSAVA / CASEWORK)
#}
{% macro ilta_lookup(source_system, map_source, raw_col) %}
SELECT feature, key, ilta
FROM (
    SELECT
        u.feature,
        u.key,
        u.ilta,
        ROW_NUMBER() OVER (PARTITION BY u.feature, u.key ORDER BY u.prio) AS rn
    FROM (
        SELECT UPPER(TRIM(feature)) AS feature, LOWER(TRIM(raw_value)) AS key,
               TRIM(ilta_label) AS ilta, 1 AS prio
        FROM {{ ref('ilta_map_supplement') }}
        WHERE source_system = '{{ source_system }}'

        UNION ALL

        SELECT
            CASE LOWER(TRIM(m.feature))
                WHEN 'age_range'                THEN 'AGE'
                WHEN 'ethnicity'                THEN 'ETHNICITY'
                WHEN 'gender'                   THEN 'GENDER'
                WHEN 'disability_text'          THEN 'DISABILITY_TYPE'
                WHEN 'disability'               THEN 'DISABILITY_TYPE'
                WHEN 'disability_n_y'           THEN 'DISABILITY_YN'
                WHEN 'do you have a disability' THEN 'DISABILITY_YN'
            END,
            LOWER(TRIM(m.{{ raw_col }})),
            TRIM(m.ilta),
            2
        FROM {{ source('reference', map_source) }} m

        UNION ALL

        SELECT feature, LOWER(TRIM(ilta_label)), TRIM(ilta_label), 3
        FROM {{ ref('ilta_categories') }}
    ) u
    INNER JOIN {{ ref('ilta_categories') }} c
        ON  c.feature = u.feature
        AND LOWER(TRIM(c.ilta_label)) = LOWER(u.ilta)
    WHERE u.feature IS NOT NULL
      AND u.key IS NOT NULL
)
WHERE rn = 1
{% endmacro %}
