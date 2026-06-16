{{
  config(
    materialized='table',
    description='AdvicePro cases flattened to one row per topic via case_topic_bridge, mapped to UT1'
  )
}}

/*
  Stage 1c: AdvicePro with case_topic_bridge exploded.

  case_topic_bridge has one row per case x topic (pre-exploded by ETL).
  Mapping chain: case_topic_bridge -> s_c_csi_map -> universal_themes_map -> UT1.
  Community Care cases key on (category, case_specific_issue);
  all others key on (supercategory, category).

  SEGMENT = UT1 from UNIVERSAL_THEMES_MAP.
    'Unmapped' — topic exists in bridge but has no UT1 match (taxonomy drift).

  Grain: one row per case x topic.
  QUERY_COUNT = 1 per row (sum gives topic mention counts, not case counts).
  HAS_LETTER = 0 — AdvicePro does not produce letters.
*/

SELECT
    c.la_name                                                                         AS LA_NAME,
    TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD')              AS QUERY_DATE,
    'AdvicePro'                                                                       AS SOURCE_SYSTEM,
    1                                                                                 AS QUERY_COUNT,
    COALESCE(u.ut1, 'Unmapped')                                                       AS SEGMENT,
    d.age_range                                                                       AS AGE_BAND,
    0                                                                                 AS HAS_LETTER,
    loc.ward                                                                          AS LOCALITY_NAME

FROM {{ source('casework', 'advicepro_casework') }} c

INNER JOIN {{ source('casework', 'case_topic_bridge') }} b
    ON c.case_reference = b.case_reference

LEFT JOIN {{ source('reference', 's_c_csi_map') }} m
    ON b.s_c_csi_id = m.s_c_csi_id

LEFT JOIN {{ source('reference', 'universal_themes_map') }} u
    ON  u.org = 'advicepro'
    AND LOWER(TRIM(u.t1)) = LOWER(TRIM(IFF(m.supercategory = 'Community Care', m.category,            m.supercategory)))
    AND LOWER(TRIM(u.t2)) = LOWER(TRIM(IFF(m.supercategory = 'Community Care', m.case_specific_issue, m.category)))

LEFT JOIN {{ source('casework', 'advicepro_demographics') }} d
    ON c.case_reference = d.case_reference

LEFT JOIN {{ source('casework', 'casework_locality') }} loc
    ON c.case_reference = loc.case_reference

WHERE c.la_name IS NOT NULL
