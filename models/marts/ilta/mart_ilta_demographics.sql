{{
  config(
    materialized='table',
    description='ILTA demographics: record counts by source x month x LA x org x feature x ILTA category. Long format for Power BI.'
  )
}}

/*
  Long-format ILTA demographics for self-serve Power BI.

  Grain: SOURCE_SYSTEM x MONTH_DATE x LA_NAME x ORG_NAME x FEATURE x ILTA_CATEGORY.
  RECORD_COUNT = AccessAva conversations / AdvicePro cases (NOT unique clients).

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
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, GENDER, AGE, DISABILITY, ETHNICITY
    FROM {{ ref('stg_ilta_accessava') }}
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, GENDER, AGE, DISABILITY, ETHNICITY
    FROM {{ ref('stg_ilta_advicepro') }}
),

long AS (
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, 'GENDER'     AS FEATURE, GENDER     AS ILTA_CATEGORY FROM records
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, 'AGE'        AS FEATURE, AGE        AS ILTA_CATEGORY FROM records
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, 'DISABILITY' AS FEATURE, DISABILITY AS ILTA_CATEGORY FROM records
    UNION ALL
    SELECT SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, 'ETHNICITY'  AS FEATURE, ETHNICITY  AS ILTA_CATEGORY FROM records
)

SELECT
    SOURCE_SYSTEM,
    MONTH_DATE,
    LA_NAME,
    ORG_NAME,
    FEATURE,
    ILTA_CATEGORY,
    FEATURE || '|' || ILTA_CATEGORY AS ILTA_KEY,
    COUNT(*)                        AS RECORD_COUNT
FROM long
GROUP BY SOURCE_SYSTEM, MONTH_DATE, LA_NAME, ORG_NAME, FEATURE, ILTA_CATEGORY
