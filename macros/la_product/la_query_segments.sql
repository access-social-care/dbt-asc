{% macro la_query_segments(months_back) %}
-- View 4: Query segments by supercategory
-- AccessAva only: other sources have NULL SEGMENT in stg_la_queries
-- TODO: Map helplines UT1 to a shared segment taxonomy for cross-source segmentation
-- Grain: one row per LA × segment within the time window
WITH base AS (
    SELECT
        LA_NAME,
        COALESCE(SEGMENT, 'Uncategorised') AS SEGMENT,
        SUM(QUERY_COUNT)                   AS RAW_COUNT
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
      AND SOURCE_SYSTEM = 'AccessAva'
    GROUP BY 1, 2
)
SELECT
    LA_NAME,
    SEGMENT,
    {{ la_suppress('RAW_COUNT') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}              AS TIME_WINDOW_MONTHS
FROM base
{% endmacro %}
