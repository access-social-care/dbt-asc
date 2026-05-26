{% macro la_query_segments(months_back, source_model='stg_la_queries') %}
-- View 4: Query segments by supercategory
-- Both sources: AdvicePro SEGMENT = NULL → appears as 'Uncategorised'.
-- TODO: Map AdvicePro case type to shared segment taxonomy for cross-source segmentation
-- TODO: Map helplines UT1 to same shared taxonomy when re-added
-- Grain: one row per LA x segment within the time window
SELECT
    LA_NAME,
    COALESCE(SEGMENT, 'Uncategorised')    AS SEGMENT,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1, 2
{% endmacro %}
