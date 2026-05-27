{{
  config(materialized='table', description='Glos query segments — 6 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    SEGMENT,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS QUERY_COUNT_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_query_segments') }}
WHERE TIME_WINDOW_MONTHS = 6
