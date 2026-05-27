{{
  config(materialized='table', description='Glos query source — 6 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    SOURCE_SYSTEM,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS QUERY_COUNT_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_query_source') }}
WHERE TIME_WINDOW_MONTHS = 6
