{{
  config(
    materialized='table',
    description='AccessAva conversations flattened to one row per CASE_SPECIFIC_ISSUES_GROUP segment'
  )
}}

/*
  Stage 1b: AccessAva conversations with CASE_SPECIFIC_ISSUES_GROUP flattened.

  CASE_SPECIFIC_ISSUES_GROUP contains a semicolon-separated list of issue categories
  (e.g. "Care needs;Funding;Legal issues"). Each conversation is expanded into
  one row per category using LATERAL FLATTEN.

  Conversations with no case_specific_issues_group are excluded — they carry
  no segment signal. For total interaction counts, use stg_accessava instead.

  SEGMENT: individual trimmed category value after splitting on ';'

  Grain: one row per conversation × segment combination.
  QUERY_COUNT = 1 per row (sum in mart to get total segment hits).
*/

SELECT
    sub.la_name                                                      AS LA_NAME,
    sub.created_at::DATE                                             AS QUERY_DATE,
    'AccessAva'                                                      AS SOURCE_SYSTEM,
    1                                                                AS QUERY_COUNT,
    sub.segment                                                      AS SEGMENT,
    sub.age                                                          AS AGE_BAND,
    CASE WHEN sub.lettercode IS NOT NULL THEN 1 ELSE 0 END           AS HAS_LETTER,
    l.ward                                                           AS LOCALITY_NAME

FROM (
    SELECT
        a.la_name,
        a.created_at,
        a.age,
        a.lettercode,
        a.transcript_id,
        TRIM(f.value::VARCHAR) AS segment
    FROM {{ source('accessava', 'accessava') }} a,
    LATERAL FLATTEN(input => SPLIT(a.case_specific_issues_group, ';'), OUTER => TRUE) f
    WHERE a.la_name IS NOT NULL
      AND a.case_specific_issues_group IS NOT NULL
      AND TRIM(f.value::VARCHAR) != ''
) sub
LEFT JOIN {{ source('accessava', 'accessava_locality') }} l
    ON sub.transcript_id = l.transcript_id
