/*
  FAIL if any "Total" metric row survives into either CLD staging model.

  The source ODS publishes a trailing Total column alongside the months, and
  the extraction repo's melt turns it into an ordinary metric row (it filters
  ROWS, not columns, before melting - so Total cannot be dropped upstream).
  A Total row is the same LA's own months added up, not an extra category:
  verified end-to-end 2026-09-14 against a real LA, whose 12 monthly values
  summed to exactly its Total row. If one leaks through, every aggregate over
  these models silently doubles - which is the exact bug found live in
  signal_processing's R code and the reason this is a test rather than a
  comment.

  Matched on METRIC_RAW so it catches "Total", "Total [p]", and any future
  variant, independently of the model's own de-bracketing logic (a test that
  reused that logic would pass whenever the logic itself was wrong).

  !! UNVERIFIED (2026-09-16) - never executed; no Snowflake connection. !!
*/

SELECT 'stg_cld_long_term_support' AS model_name, metric_raw, COUNT(*) AS n_rows
FROM {{ ref('stg_cld_long_term_support') }}
WHERE UPPER(metric_raw) LIKE '%TOTAL%'
GROUP BY 1, 2

UNION ALL

SELECT 'stg_cld_assessments' AS model_name, metric_raw, COUNT(*) AS n_rows
FROM {{ ref('stg_cld_assessments') }}
WHERE UPPER(metric_raw) LIKE '%TOTAL%'
GROUP BY 1, 2
