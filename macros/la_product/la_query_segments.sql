{% macro la_query_segments(months_back, source_model='stg_la_topic_mentions_glos', suppress=true) %}
-- Grain: one row per LA x segment within the time window.
-- suppress=false: returns raw QUERY_COUNT_RAW for intermediate/BI use.
SELECT
    LA_NAME,
    COALESCE(SEGMENT, 'Uncategorised')    AS SEGMENT,
    {% if suppress %}
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {% else %}
    SUM(QUERY_COUNT) AS QUERY_COUNT_RAW,
    {% endif %}
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1, 2
{% endmacro %}
