/*
  FAIL if any non-Total metric in the CLD landing tables does not parse into a
  real month.

  Tested against the LANDING SOURCE, not the models: the models hold back
  unparseable rows (period_start IS NULL) so they never carry a NULL period
  downstream. That hold-back is only safe if something else guarantees the set
  it discards is empty - otherwise a month header whose format changed (say a
  quarter published as "2025-07" instead of "July 2025") would vanish from the
  models with no error anywhere. This test is that guarantee.

  Parsing logic is duplicated from the models on purpose: the point is to
  detect a header shape the models cannot handle, so it must apply the same
  cast the models apply.

  !! UNVERIFIED (2026-09-16) - never executed; no Snowflake connection. !!
*/

WITH all_landing AS (

    SELECT 'cld_long_term_support' AS dataset_id, metric
    FROM {{ source('data_portal_landing', 'cld_long_term_support') }}

    UNION ALL

    SELECT 'cld_assessments' AS dataset_id, metric
    FROM {{ source('data_portal_landing', 'cld_assessments') }}

),

labelled AS (

    SELECT
        dataset_id,
        metric,
        TRIM(
            IFF(metric ILIKE '%[p]', LEFT(metric, LENGTH(metric) - 3), metric)
        ) AS metric_label
    FROM all_landing

)

SELECT DISTINCT
    dataset_id,
    metric,
    metric_label
FROM labelled
WHERE UPPER(metric_label) NOT LIKE 'TOTAL%'
  AND TRY_TO_DATE(
        LEFT(SPLIT_PART(metric_label, ' ', 1), 3)
        || ' ' || SPLIT_PART(metric_label, ' ', 2)
        || ' 01',
        'MON YYYY DD'
      ) IS NULL
