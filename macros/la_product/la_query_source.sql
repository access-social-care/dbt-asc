{% macro la_query_source(months_back) %}
-- View 5: Source breakdown - same combined dataset as all other views,
-- with SOURCE_SYSTEM added as a dimension.
-- Grain: one row per LA × source system within the time window
WITH base AS (
    SELECT
        LA_NAME,
        SOURCE_SYSTEM,
        SUM(QUERY_COUNT) AS RAW_COUNT
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
    GROUP BY 1, 2
)
SELECT
    LA_NAME,
    SOURCE_SYSTEM,
    {{ la_suppress('RAW_COUNT') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}              AS TIME_WINDOW_MONTHS
FROM base
{% endmacro %}
