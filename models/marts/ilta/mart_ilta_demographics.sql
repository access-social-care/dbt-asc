{{
  config(
    materialized='table',
    description='ILTA demographics: record counts by source x month x LA x org x feature x ILTA category. Long format for Power BI.'
  )
}}

/*
  Long-format ILTA demographics for self-serve Power BI.

  Grain: SOURCE_SYSTEM x MONTH_DATE x LA_NAME x ORG_NAME x FEATURE x ILTA_CATEGORY.
  RECORD_COUNT = AccessAva conversations + AdvicePro people with care needs
  (persons on each case, from report HS6T5CH9). NOT unique clients; AdvicePro
  rows have no LA or member (NULL LA_NAME, fixed ORG_NAME).

  Every record appears exactly once per FEATURE, so for any FEATURE the sum of
  RECORD_COUNT over all categories equals the total record count. Status
  categories ('Not recorded', 'Unmapped', 'Not collected') are kept as rows so
  completeness ("do you consistently collect X?") is computable:
      completeness = 1 - Not recorded / (total - Not collected)

  FEATURE values match ilta_categories (GENDER, AGE, DISABILITY, ETHNICITY).
  ILTA_KEY = FEATURE || '|' || ILTA_CATEGORY, joins to dim_ilta_category.

  LA_NAME is NULL for most AccessAva rows (upstream: tenant has no LA) and
  some AdvicePro rows; kept, not dropped. NOT small-number suppressed - the
  LA x month x category grain can yield counts of 1-2. Restrict who is granted
  the ilta schema, or aggregate away LA/month in the report for wider audiences.
*/

WITH records AS (
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, RECORD_WEIGHT, GENDER, AGE, DISABILITY, ETHNICITY
    FROM {{ ref('stg_ilta_accessava') }}
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, RECORD_WEIGHT, GENDER, AGE, DISABILITY, ETHNICITY
    FROM {{ ref('stg_ilta_advicepro') }}
),

long AS (
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, RECORD_WEIGHT, 'GENDER'     AS FEATURE, GENDER     AS ILTA_CATEGORY FROM records
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, RECORD_WEIGHT, 'AGE'        AS FEATURE, AGE        AS ILTA_CATEGORY FROM records
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, RECORD_WEIGHT, 'DISABILITY' AS FEATURE, DISABILITY AS ILTA_CATEGORY FROM records
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, RECORD_WEIGHT, 'ETHNICITY'  AS FEATURE, ETHNICITY  AS ILTA_CATEGORY FROM records
)

SELECT
    SOURCE_SYSTEM,
    MONTH_DATE,
    LA_NAME,
    ORG_NAME,
    FEATURE,
    ILTA_CATEGORY,
    FEATURE || '|' || ILTA_CATEGORY AS ILTA_KEY,
    SUM(RECORD_WEIGHT)              AS RECORD_COUNT
FROM long
GROUP BY SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, FEATURE, ILTA_CATEGORY
