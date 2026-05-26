{% macro la_activity_summary(months_back, source_model='stg_la_queries') %}
-- View 2: LA activity summary - total queries across all sources
-- Grain: one row per LA for the full time window
SELECT
    LA_NAME,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1
{% endmacro %}
