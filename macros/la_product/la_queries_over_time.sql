{% macro la_queries_over_time(months_back) %}
-- View 3: Queries over time - monthly time series across all sources
-- Grain: one row per LA × calendar month
WITH base AS (
    SELECT
        LA_NAME,
        DATE_TRUNC('MONTH', QUERY_DATE)::DATE   AS QUERY_MONTH,
        SUM(QUERY_COUNT)                        AS RAW_COUNT
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
    GROUP BY 1, 2
)
SELECT
    LA_NAME,
    QUERY_MONTH,
    {{ la_suppress('RAW_COUNT') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}              AS TIME_WINDOW_MONTHS
FROM base
ORDER BY LA_NAME, QUERY_MONTH
{% endmacro %}
