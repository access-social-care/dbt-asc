{% macro la_activity_summary(months_back, source_model='stg_la_queries', suppress=true) %}
-- View 2: LA activity summary - total queries across all sources
-- Grain: one row per LA for the full time window
-- suppress=false: returns raw QUERY_COUNT_RAW for intermediate/BI use
SELECT
    LA_NAME,
    {% if suppress %}
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {% else %}
    SUM(QUERY_COUNT) AS QUERY_COUNT_RAW,
    {% endif %}
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1
{% endmacro %}
