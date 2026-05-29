{{
  config(
    materialized='table',
    description='AdvicePro cases with demographics and locality — Stage 1 of LA Data Product staging'
  )
}}

/*
  Stage 1: Join AdvicePro casework + demographics + locality into a single row per case.

  Three LEFT JOINs on case_reference:
    - advicepro_casework  → core case record (LA, date, reference)
    - advicepro_demographics → client age/agegroup (NULL if not recorded)
    - casework_locality   → sub-LA ward from postcode lookup (NULL if postcode missing/unresolved)

  Output columns match the stg_la_queries interface so it can be unioned with AccessAva directly.

  TODOs (resolve after confirming with the data owner):
    - locality grain: currently using ward — alternatives are lso_area_name (LSOA) or mso_area_name (MSOA)
    - additional demographic columns available: gender, do_you_have_a_disability, ethnic_origin
      these are not yet surfaced in stg_la_queries but exist in ADVICEPRO_DEMOGRAPHICS if needed
*/

SELECT
    c.la_name                                                                          AS LA_NAME,
    TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD')              AS QUERY_DATE,
    'AdvicePro'                                                                       AS SOURCE_SYSTEM,
    1                                                                                 AS QUERY_COUNT,
    NULL::VARCHAR                                                                     AS SEGMENT,
    d.age_range                                                                        AS AGE_BAND,
    0                                                                                 AS HAS_LETTER,
    loc.ward                                                                          AS LOCALITY_NAME  -- TODO: confirm preferred grain (ward / lso_area_name / mso_area_name)

FROM {{ source('casework', 'advicepro_casework') }} c

LEFT JOIN {{ source('casework', 'advicepro_demographics') }} d
    ON c.case_reference = d.case_reference

LEFT JOIN {{ source('casework', 'casework_locality') }} loc
    ON c.case_reference = loc.case_reference

WHERE c.la_name IS NOT NULL
