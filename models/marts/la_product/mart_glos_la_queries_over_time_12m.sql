{{
  config(materialized='table', description='Glos queries over time — 12 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    QUERY_MONTH,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS QUERY_COUNT_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_queries_over_time') }}
WHERE TIME_WINDOW_MONTHS = 12
