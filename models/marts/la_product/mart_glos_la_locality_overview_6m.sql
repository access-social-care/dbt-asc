{{
  config(materialized='table', description='Glos locality overview — 6 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    LOCALITY_NAME,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS QUERY_COUNT_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_locality_overview') }}
WHERE TIME_WINDOW_MONTHS = 6
