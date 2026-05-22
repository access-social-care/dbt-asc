{% macro la_locality_overview(months_back) %}
-- View 1: Localities overview
-- Grain: one row per LA × locality within the time window
-- AccessAva only: other sources have NULL LOCALITY_NAME in stg_la_queries
WITH base AS (
    SELECT
        LA_NAME,
        LOCALITY_NAME,
        SUM(QUERY_COUNT) AS RAW_COUNT
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
      AND SOURCE_SYSTEM = 'AccessAva'
    GROUP BY 1, 2
)
SELECT
    LA_NAME,
    LOCALITY_NAME,
    {{ la_suppress('RAW_COUNT') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}              AS TIME_WINDOW_MONTHS
FROM base
{% endmacro %}
