{{
  config(
    materialized='table',
    description='AdvicePro persons-with-care-needs demographics for the ILTA return. One row per report row, weighted by persons.'
  )
}}

/*
  AdvicePro side of the ILTA return, from report HS6T5CH9 (loaded to
  CASEWORK.PUBLIC.ADVICEPRO_DEMOGRAPHICS_PERSONS by
  loaders/load_advicepro_persons_to_snowflake.R). Chosen 2026-10-12 over
  FD7DXGL4 (ADVICEPRO_DEMOGRAPHICS): HS6T5CH9 is roughly one row per case
  and covers almost every case; FD7DXGL4 covers only the cases that have an
  equalities form (about a quarter).

  Grain: one row per report row. Since Feb 2026 the report COLLAPSES identical
  rows into one row with a count in "Number of Person with Care Needs" (50 rows,
  all fully blank: no age, gender/ethnicity '[Not Specified]', disability 'No';
  none before Feb 2026). Each case is one person, so the count is the number
  of identical cases collapsed together, not people per case (checked
  2026-10-13: Jul 2026 has 42 cases in ADVICEPRO_CASEWORK, 19 report rows and
  44 in the count). RECORD_WEIGHT = that count, which restores the collapsed
  rows: SUM(RECORD_WEIGHT) counts people (= cases). Without it recent months
  would be undercounted. No demographic profile is spread over several people:
  every collapsed row is blank.

  LIMITS (report has no case reference): LA_NAME is NULL and ORG_NAME is a
  fixed label, so AdvicePro rows cannot be sliced by local authority or member
  organisation, and cannot be joined to ADVICEPRO_CASEWORK. Report rows only
  start in January 2025.

  Status values and the DISABILITY derivation work as in stg_ilta_accessava,
  with one AdvicePro-specific rule (agreed 2026-10-12): AdvicePro forces a
  Yes/No answer to the disability question, so 'No' is often just the default
  (in Apr-Sep 2025, 155 of 161 'No' answers had age, gender and ethnicity all
  blank). A 'No' only counts as 'No disability' when at least one of
  age/gender/ethnicity was answered; otherwise it is 'Not recorded'.
  'Yes' with no type is 'Other'. The loader turns '[Not Specified]' into NULL.
*/

WITH lk AS (
    {{ ilta_lookup('AdvicePro', 'ilta_map_casework', 'casework') }}
)

SELECT
    TO_VARCHAR(d.record_id)                                 AS RECORD_ID,
    'AdvicePro'                                             AS SOURCE_SYSTEM,
    DATE_TRUNC('month', d.case_open_date)::DATE             AS MONTH_DATE,
    NULL::VARCHAR                                           AS LA_NAME,
    'AdvicePro (no member in report)'                       AS ORG_NAME,
    d.persons                                               AS RECORD_WEIGHT,

    CASE WHEN NULLIF(TRIM(d.gender), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_gen.ilta, 'Unmapped') END          AS GENDER,
    CASE WHEN NULLIF(TRIM(d.age_range), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_age.ilta, 'Unmapped') END          AS AGE,
    CASE
        WHEN l_yn.ilta = 'No' AND (
                 COALESCE(TRIM(d.age_range), '')    NOT IN ('', 'Not Stated')
              OR COALESCE(TRIM(d.gender), '')       NOT IN ('', 'Not Stated')
              OR COALESCE(TRIM(d.ethnic_origin), '') NOT IN ('', 'Not Stated'))
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

FROM {{ source('casework', 'advicepro_demographics_persons') }} d
LEFT JOIN lk l_gen ON l_gen.feature = 'GENDER'          AND l_gen.key = LOWER(TRIM(d.gender))
LEFT JOIN lk l_age ON l_age.feature = 'AGE'             AND l_age.key = LOWER(TRIM(d.age_range))
LEFT JOIN lk l_eth ON l_eth.feature = 'ETHNICITY'       AND l_eth.key = LOWER(TRIM(d.ethnic_origin))
LEFT JOIN lk l_dt  ON l_dt.feature  = 'DISABILITY_TYPE' AND l_dt.key  = LOWER(TRIM(d.what_type_of_disability_do_you_consider_you_have))
LEFT JOIN lk l_yn  ON l_yn.feature  = 'DISABILITY_YN'   AND l_yn.key  = LOWER(TRIM(d.do_you_have_a_disability))
