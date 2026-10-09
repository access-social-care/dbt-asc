{{
  config(
    materialized='table',
    description='ILTA category dimension (reported categories + status categories) with sort order, for Power BI.'
  )
}}

/*
  One row per FEATURE x category the mart can emit, including categories with
  zero records (so visuals show the full ILTA form shape) and the status
  categories. IS_REPORTED = true for the categories that appear on the ILTA
  return; false for status rows (Not recorded / Unmapped / Not collected).
  Only the four reported FEATUREs are included (not the DISABILITY_TYPE /
  DISABILITY_YN helper features).
*/

SELECT
    feature                          AS FEATURE,
    ilta_label                       AS ILTA_CATEGORY,
    feature || '|' || ilta_label     AS ILTA_KEY,
    sort_order                       AS SORT_ORDER,
    TRUE                             AS IS_REPORTED
FROM {{ ref('ilta_categories') }}
WHERE is_reported

UNION ALL

SELECT f.feature, s.status, f.feature || '|' || s.status, s.sort_order, FALSE
FROM (
    SELECT DISTINCT feature FROM {{ ref('ilta_categories') }} WHERE is_reported
) f
CROSS JOIN (
    SELECT 'Not recorded' AS status, 90 AS sort_order
    UNION ALL SELECT 'Unmapped', 91
    UNION ALL SELECT 'Not collected', 92
) s
