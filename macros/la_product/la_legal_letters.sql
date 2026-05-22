{% macro la_legal_letters(months_back) %}
-- View 7: Legal letters - total queries vs letters generated
-- AccessAva only: only source with HAS_LETTER dimension in stg_la_queries
-- Grain: one row per LA for the full time window
WITH base AS (
    SELECT
        LA_NAME,
        SUM(QUERY_COUNT) AS TOTAL_QUERIES_RAW,
        SUM(HAS_LETTER)  AS TOTAL_LETTERS_RAW
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
      AND SOURCE_SYSTEM = 'AccessAva'
    GROUP BY 1
)
SELECT
    LA_NAME,
    {{ la_suppress('TOTAL_QUERIES_RAW') }}  AS TOTAL_QUERIES_DISPLAY,
    {{ la_suppress('TOTAL_LETTERS_RAW') }}  AS TOTAL_LETTERS_DISPLAY,
    {{ months_back }}                       AS TIME_WINDOW_MONTHS
FROM base
{% endmacro %}
