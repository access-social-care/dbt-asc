{% macro la_queries_over_time(months_back, source_model='stg_la_queries') %}
-- View 3: Queries over time - monthly time series across all sources
-- Grain: one row per LA x calendar month
-- Note: ORDER BY omitted - ORDER BY inside UNION ALL is invalid SQL;
--       consumers should order in their own queries
SELECT
    LA_NAME,
    DATE_TRUNC('MONTH', QUERY_DATE)::DATE AS QUERY_MONTH,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1, 2
{% endmacro %}
