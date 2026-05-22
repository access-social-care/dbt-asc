{{
  config(
    materialized='table',
    description='AccessAva chatbot conversations — Stage 1 of LA Data Product staging'
  )
}}

/*
  Stage 1: Normalize AccessAva raw data to the stg_la_queries column interface.

  LA name: a."la_name" — the LA that deployed the AccessAva chatbot.
    (Not tenant_name — that is the tenancy identifier, not the CASSR name.)

  LOCALITY_NAME: ward from ACCESSAVA_LOCALITY joined on transcript_id.
    Matches the locality grain used by stg_advicepro (ward from casework_locality).
    ACCESSAVA_LOCALITY also has lso_area_name, mso_area_name, ward_code etc. if finer
    or coarser grain is needed later.

  HAS_LETTER: 1 if letterCode is populated (a formal letter was generated), else 0.
    Only AccessAva produces this dimension — AdvicePro rows have HAS_LETTER = 0.

  SEGMENT / AGE_BAND: populated directly from AccessAva fields.
    AdvicePro SEGMENT = NULL (no shared segment taxonomy yet).
    AdvicePro AGE_BAND = age_range (different field, same concept).
*/

SELECT
    a."la_name"                                                    AS LA_NAME,
    a."created_at"::DATE                                           AS QUERY_DATE,
    'AccessAva'                                                    AS SOURCE_SYSTEM,
    1                                                              AS QUERY_COUNT,
    a."categories"                                                 AS SEGMENT,
    a."age"                                                        AS AGE_BAND,
    CASE WHEN a."letterCode" IS NOT NULL THEN 1 ELSE 0 END         AS HAS_LETTER,
    l."ward"                                                       AS LOCALITY_NAME

FROM {{ source('accessava', 'accessava') }} a
LEFT JOIN {{ source('accessava', 'accessava_locality') }} l
    ON a."transcript_id" = l."transcript_id"
WHERE a."la_name" IS NOT NULL
