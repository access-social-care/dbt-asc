{{
  config(materialized='table', description='Glos demographics — 3 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    AGE_BAND,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS QUERY_COUNT_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_demographics') }}
WHERE TIME_WINDOW_MONTHS = 3
