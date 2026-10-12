{{
  config(
    materialized='table',
    description='AdvicePro cases with ILTA-mapped demographics. One row per case.'
  )
}}

/*
  One row per AdvicePro case (case_reference) with the four ILTA reporting
  dimensions resolved to ILTA labels. Status values work as in
  stg_ilta_accessava.

  DISABILITY differs from AccessAva in one rule (agreed 2026-10-12): AdvicePro
  records 'No' to "Do you have a disability" as a default - in Apr-Sep 2025,
  155 of 161 'No' answers had age, gender and ethnicity all blank. A 'No' only
  counts as 'No disability' when at least one of age/gender/ethnicity was
  answered; otherwise it is 'Not recorded'. (In AccessAva the user answers
  the question, so 'No' is always taken at face value.)

  Demographics come from CASEWORK.PUBLIC.ADVICEPRO_DEMOGRAPHICS (AdvicePro
  report FD7DXGL4), which covers only a small share of cases. A case with no
  row there is 'Not recorded' on all four dimensions (LEFT JOIN).

  Counts are cases, not unique clients.
*/

WITH lk AS (
    {{ ilta_lookup('AdvicePro', 'ilta_map_casework', 'casework') }}
)

SELECT
    c.case_reference                                        AS RECORD_ID,
    'AdvicePro'                                             AS SOURCE_SYSTEM,
    TRY_TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD') AS MONTH_DATE,
    c.la_name                                               AS LA_NAME,
    c.member_canonical_name                                 AS ORG_NAME,

    CASE WHEN NULLIF(TRIM(d.gender), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_gen.ilta, 'Unmapped') END          AS GENDER,
    CASE WHEN NULLIF(TRIM(d.age_range), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_age.ilta, 'Unmapped') END          AS AGE,
    CASE
        WHEN l_yn.ilta = 'No' AND (
                 COALESCE(TRIM(d.age_range), '')    NOT IN ('', '[Not Specified]', 'Not Stated')
              OR COALESCE(TRIM(d.gender), '')       NOT IN ('', '[Not Specified]', 'Not Stated')
              OR COALESCE(TRIM(d.ethnic_origin), '') NOT IN ('', '[Not Specified]', 'Not Stated'))
                                                            THEN 'No disability'
        WHEN NULLIF(TRIM(d.what_type_of_disability_do_you_consider_you_have), '') IS NOT NULL
                                                            THEN COALESCE(l_dt.ilta, 'Unmapped')
        WHEN l_yn.ilta = 'Yes'                              THEN 'Other'
        -- default 'No': nothing else about the client was answered
        WHEN l_yn.ilta = 'No'                               THEN 'Not recorded'
        WHEN NULLIF(TRIM(d.do_you_have_a_disability), '') IS NOT NULL THEN 'Unmapped'
        ELSE 'Not recorded'
    END                                                     AS DISABILITY,
    CASE WHEN NULLIF(TRIM(d.ethnic_origin), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_eth.ilta, 'Unmapped') END          AS ETHNICITY,

    NULLIF(TRIM(d.gender), '')                              AS GENDER_RAW,
    NULLIF(TRIM(d.age_range), '')                           AS AGE_RAW,
    NULLIF(TRIM(d.do_you_have_a_disability), '')            AS DISABILITY_YN_RAW,
    NULLIF(TRIM(d.what_type_of_disability_do_you_consider_you_have), '') AS DISABILITY_TYPE_RAW,
    NULLIF(TRIM(d.ethnic_origin), '')                       AS ETHNICITY_RAW

FROM {{ source('casework', 'advicepro_casework') }} c
LEFT JOIN {{ source('casework', 'advicepro_demographics') }} d
    ON c.case_reference = d.case_reference
LEFT JOIN lk l_gen ON l_gen.feature = 'GENDER'          AND l_gen.key = LOWER(TRIM(d.gender))
LEFT JOIN lk l_age ON l_age.feature = 'AGE'             AND l_age.key = LOWER(TRIM(d.age_range))
LEFT JOIN lk l_eth ON l_eth.feature = 'ETHNICITY'       AND l_eth.key = LOWER(TRIM(d.ethnic_origin))
LEFT JOIN lk l_dt  ON l_dt.feature  = 'DISABILITY_TYPE' AND l_dt.key  = LOWER(TRIM(d.what_type_of_disability_do_you_consider_you_have))
LEFT JOIN lk l_yn  ON l_yn.feature  = 'DISABILITY_YN'   AND l_yn.key  = LOWER(TRIM(d.do_you_have_a_disability))
