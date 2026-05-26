{% macro la_query_source(months_back, source_model='stg_la_queries') %}
-- View 5: Source breakdown - same combined dataset as all other views,
-- with SOURCE_SYSTEM added as a dimension.
-- Grain: one row per LA x source system within the time window
SELECT
    LA_NAME,
    SOURCE_SYSTEM,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1, 2
{% endmacro %}
