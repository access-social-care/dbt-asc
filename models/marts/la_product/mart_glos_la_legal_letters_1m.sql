{{
  config(materialized='table', description='Glos legal letters — 1 month window, small-number suppressed')
}}
SELECT
    LA_NAME,
    {{ la_suppress('QUERY_COUNT_RAW') }} AS TOTAL_QUERIES_DISPLAY,
    {{ la_suppress('HAS_LETTER_RAW') }}  AS TOTAL_LETTERS_DISPLAY,
    TIME_WINDOW_MONTHS
FROM {{ ref('int_glos_la_legal_letters') }}
WHERE TIME_WINDOW_MONTHS = 1
