{{
  config(
    materialized='table',
    description='Combined LA query data: AccessAva + AdvicePro (Stage 2 of LA Data Product staging)'
  )
}}

/*
  Stage 2: UNION ALL across service lines.
  Each source is pre-staged to a common interface:
    - stg_advicepro  → AdvicePro cases (casework + demographics + locality joined)
    - accessava CTE  → AccessAva chatbot conversations

  All macros in models/marts/la_product/ reference this model.
  Helplines excluded from PoC — re-add when UT1 → shared segment taxonomy is agreed.

  LA name harmonisation NOT implemented in PoC:
    AccessAva uses tenant_name; AdvicePro uses local_authority (CASSR-standardised by ETL).

  TODO: Confirm column name LOCALITY_NAME in accessava_locality table.
*/

WITH accessava AS (
    SELECT
        a."tenant_name"                                              AS LA_NAME,
        a."created_at"::DATE                                         AS QUERY_DATE,
        'AccessAva'                                                  AS SOURCE_SYSTEM,
        1                                                            AS QUERY_COUNT,
        a."categories"                                               AS SEGMENT,
        a."age"                                                      AS AGE_BAND,
        CASE WHEN a."letterCode" IS NOT NULL THEN 1 ELSE 0 END       AS HAS_LETTER,
        COALESCE(l.LOCALITY_NAME, 'Unknown')                         AS LOCALITY_NAME
    FROM {{ source('accessava', 'accessava') }} a
    LEFT JOIN {{ source('accessava', 'accessava_locality') }} l
        ON a."tenant_id" = l.TENANT_ID
    WHERE a."tenant_name" IS NOT NULL
),

advicepro AS (
    SELECT * FROM {{ ref('stg_advicepro') }}
)

SELECT * FROM accessava
UNION ALL
SELECT * FROM advicepro
