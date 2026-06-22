{{
  config(
    materialized='table',
    description='AdvicePro cases with demographics and locality — Stage 1 of LA Data Product staging'
  )
}}

/*
  Stage 1: AdvicePro casework flattened to one row per case x case_specific_issues_group value.

  case_specific_issues_group is semicolon-joined in advicepro_casework.
  LATERAL FLATTEN explodes it so SEGMENT = individual raw category value.

  This is the "raw category" track — SEGMENT is the source value, not mapped to UT1.
  For UT1-mapped output (comparable across AccessAva and Helplines), use stg_advicepro_segments.

  Grain: one row per case x segment value.
  QUERY_COUNT = 1 per row (sum gives topic mention counts, not case counts).
*/

SELECT
    c.la_name                                                                         AS LA_NAME,
    TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD')             AS QUERY_DATE,
    'AdvicePro'                                                                       AS SOURCE_SYSTEM,
    1                                                                                 AS QUERY_COUNT,
    c.segment_value                                                                   AS SEGMENT,
    d.age_range                                                                       AS AGE_BAND,
    0                                                                                 AS HAS_LETTER,
    loc.ward                                                                          AS LOCALITY_NAME

FROM (
    SELECT
        la_name,
        case_open_month,
        case_reference,
        TRIM(f.value::VARCHAR)                                                        AS segment_value
    FROM {{ source('casework', 'advicepro_casework') }},
    LATERAL FLATTEN(
        INPUT => SPLIT(case_specific_issues_group, ';'),
        OUTER => TRUE
    ) f
    WHERE la_name IS NOT NULL
) c

LEFT JOIN {{ source('casework', 'advicepro_demographics') }} d
    ON c.case_reference = d.case_reference

LEFT JOIN {{ source('casework', 'casework_locality') }} loc
    ON c.case_reference = loc.case_reference
