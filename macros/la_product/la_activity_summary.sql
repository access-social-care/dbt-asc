{% macro la_activity_summary(months_back) %}
-- View 2: LA activity summary - total queries across all sources
-- Grain: one row per LA for the full time window
WITH base AS (
    SELECT
        LA_NAME,
        SUM(QUERY_COUNT) AS RAW_COUNT
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
    GROUP BY 1
)
SELECT
    LA_NAME,
    {{ la_suppress('RAW_COUNT') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}              AS TIME_WINDOW_MONTHS
FROM base
{% endmacro %}
