{{
  config(
    materialized='table',
    description='AccessAva chatbot conversations — Stage 1 of LA Data Product staging, raw-category track'
  )
}}

/*
  Stage 1 (raw-category track): AccessAva with categories exploded.

  categories is semicolon-space-joined (e.g. "Housing; Benefits; Legal").
  LATERAL FLATTEN produces one row per conversation x category value.
  SEGMENT = individual raw category value (not mapped to UT1).

  This is the raw-category track companion to stg_advicepro.
  For UT1-mapped output comparable across all three sources, use stg_accessava_segments.

  Grain: one row per conversation x category.
  QUERY_COUNT = 1 per row (sum gives category mention counts, not conversation counts).

  HAS_LETTER: 1 if letterCode is populated (a formal letter was generated), else 0.
    Only AccessAva produces this dimension — AdvicePro rows have HAS_LETTER = 0.
*/

SELECT
    a.la_name                                                      AS LA_NAME,
    a.created_at::DATE                                             AS QUERY_DATE,
    'AccessAva'                                                    AS SOURCE_SYSTEM,
    1                                                              AS QUERY_COUNT,
    TRIM(f.value::VARCHAR)                                         AS SEGMENT,
    a.age                                                          AS AGE_BAND,
    CASE WHEN a.lettercode IS NOT NULL THEN 1 ELSE 0 END           AS HAS_LETTER,
    l.ward                                                         AS LOCALITY_NAME

FROM (
    SELECT
        la_name,
        created_at,
        age,
        lettercode,
        transcript_id,
        categories
    FROM {{ source('accessava', 'accessava') }}
    WHERE la_name IS NOT NULL
) a,
LATERAL FLATTEN(
    INPUT => SPLIT(a.categories, '; '),
    OUTER => TRUE
) f
LEFT JOIN {{ source('accessava', 'accessava_locality') }} l
    ON a.transcript_id = l.transcript_id
