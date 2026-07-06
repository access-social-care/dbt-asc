{{
  config(
    materialized='table',
    description='AccessAva conversations flattened to one row per topic_entry_point mention, mapped to UT1/UT2'
  )
}}

/*
  Stage 1: AccessAva with topic_entry_point flattened.

  topic_entry_point is semicolon-space-joined (e.g. "Housing; Benefits; Legal").
  Each conversation expands to one row per topic.

  SEGMENT = UT1 from TOPIC_ENTRY_POINT_MAP.
    'Unmatched' — topic_entry_point was NULL.
    'Unmapped'  — value exists but has no row in the map (taxonomy drift).
  UT2 = second-level theme from the same map (sparse; NULL where not applicable).

  Grain: one row per conversation x topic.
  QUERY_COUNT = 1 per row (sum gives topic mention counts, not conversation counts).
*/

SELECT
    a.la_name                                                               AS LA_NAME,
    a.created_at::DATE                                                      AS QUERY_DATE,
    'AccessAva'                                                             AS SOURCE_SYSTEM,
    1                                                                       AS QUERY_COUNT,
    CASE
        WHEN a.topic_entry_point IS NULL THEN 'Unmatched'
        ELSE COALESCE(m.ut1, 'Unmapped')
    END                                                                     AS SEGMENT,
    NULLIF(m.ut2, 'NA')                                                     AS UT2,
    a.age                                                                   AS AGE_BAND,
    CASE WHEN a.lettercode IS NOT NULL THEN 1 ELSE 0 END                    AS HAS_LETTER,
    l.county                                                                AS LOCALITY_NAME

FROM (
    SELECT
        la_name,
        created_at,
        age,
        lettercode,
        transcript_id,
        topic_entry_point,
        TRIM(f.value::VARCHAR)                                              AS topic_value
    FROM {{ source('accessava', 'accessava') }},
    LATERAL FLATTEN(
        INPUT  => SPLIT(topic_entry_point, '; '),
        OUTER  => TRUE
    ) f
    WHERE la_name IS NOT NULL
) a
LEFT JOIN {{ source('accessava', 'accessava_locality') }} l
    ON a.transcript_id = l.transcript_id
LEFT JOIN {{ source('reference', 'topic_entry_point_map') }} m
    ON LOWER(a.topic_value) = LOWER(TRIM(m.topic_entry_point))
