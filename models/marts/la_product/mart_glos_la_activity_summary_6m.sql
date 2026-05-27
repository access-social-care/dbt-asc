{{
  config(materialized='table', description='Glos activity summary — 6 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS QUERY_COUNT_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_activity_summary') }}
WHERE TIME_WINDOW_MONTHS = 6
