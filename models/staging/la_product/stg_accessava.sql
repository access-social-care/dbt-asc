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

  LOCALITY_NAME: requires sub-LA geography from ACCESSAVA_LOCALITY.
    The postcode lookup (chatbot_locality()) resolves postcodes to geography levels
    (ward, district, LSOA, MSOA). Which field maps to the PoC locality grain
    needs confirming via: DESCRIBE TABLE ACCESSAVA.PUBLIC.ACCESSAVA_LOCALITY;
    Set NULL until confirmed — do not use la_name (that is the LA name, not a locality).

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
    NULL::VARCHAR                                                  AS LOCALITY_NAME  -- TODO: join ACCESSAVA_LOCALITY on transcript_id once sub-LA field confirmed

FROM {{ source('accessava', 'accessava') }} a
WHERE a."la_name" IS NOT NULL
