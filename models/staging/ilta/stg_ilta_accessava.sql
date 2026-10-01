{{
  config(
    materialized='table',
    description='AccessAva conversations with ILTA-mapped demographics. One row per conversation.'
  )
}}

/*
  One row per AccessAva conversation (transcript_id) with the four ILTA
  reporting dimensions resolved to ILTA labels.

  Status values (instead of a label) in GENDER/AGE/DISABILITY/ETHNICITY:
    'Not recorded'  - the field is empty for this conversation
    'Unmapped'      - the field has a value with no valid ILTA mapping (map gap or
                      corrupted map row - see ilta_lookup macro). Kept visible.
    'Not collected' - the source does not hold this field at all (gender)

  DISABILITY combines two raw fields: disability_n_y (Yes/No) and
  disability_text (type). 'No' -> 'No disability'; a stated type -> its mapped
  label; 'Yes' with no type -> 'Other' (disability confirmed, type not given -
  a modelling choice, ILTA has no unspecified bucket).

  Demographics are <5% populated in AccessAva; most rows are 'Not recorded'.
  The coarse AGE column (Under 18 / 18-64 / 65+) is NOT used: it cannot be
  mapped to ILTA bands. Only AGE_RANGE is.

  Counts are conversations, not unique clients.
*/

WITH lk AS (
    {{ ilta_lookup('AccessAva', 'ilta_map_ava', 'accessava') }}
)

SELECT
    a.transcript_id                                         AS RECORD_ID,
    'AccessAva'                                             AS SOURCE_SYSTEM,
    DATE_TRUNC('month', a.created_at)::DATE                 AS MONTH_DATE,
    a.la_name                                               AS LA_NAME,
    a.tenant_name                                           AS ORG_NAME,

    'Not collected'                                         AS GENDER,
    CASE WHEN NULLIF(TRIM(a.age_range), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_age.ilta, 'Unmapped') END          AS AGE,
    CASE
        WHEN l_yn.ilta = 'No'                               THEN 'No disability'
        WHEN NULLIF(TRIM(a.disability_text), '') IS NOT NULL THEN COALESCE(l_dt.ilta, 'Unmapped')
        WHEN l_yn.ilta = 'Yes'                              THEN 'Other'
        WHEN NULLIF(TRIM(a.disability_n_y), '') IS NOT NULL THEN 'Unmapped'
        ELSE 'Not recorded'
    END                                                     AS DISABILITY,
    CASE WHEN NULLIF(TRIM(a.ethnicity), '') IS NULL THEN 'Not recorded'
         ELSE COALESCE(l_eth.ilta, 'Unmapped') END          AS ETHNICITY,

    NULL::VARCHAR                                           AS GENDER_RAW,
    NULLIF(TRIM(a.age_range), '')                           AS AGE_RAW,
    NULLIF(TRIM(a.disability_n_y), '')                      AS DISABILITY_YN_RAW,
    NULLIF(TRIM(a.disability_text), '')                     AS DISABILITY_TYPE_RAW,
    NULLIF(TRIM(a.ethnicity), '')                           AS ETHNICITY_RAW

FROM {{ source('accessava', 'accessava') }} a
LEFT JOIN lk l_age ON l_age.feature = 'AGE'             AND l_age.key = LOWER(TRIM(a.age_range))
LEFT JOIN lk l_eth ON l_eth.feature = 'ETHNICITY'       AND l_eth.key = LOWER(TRIM(a.ethnicity))
LEFT JOIN lk l_dt  ON l_dt.feature  = 'DISABILITY_TYPE' AND l_dt.key  = LOWER(TRIM(a.disability_text))
LEFT JOIN lk l_yn  ON l_yn.feature  = 'DISABILITY_YN'   AND l_yn.key  = LOWER(TRIM(a.disability_n_y))
