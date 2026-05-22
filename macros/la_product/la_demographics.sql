{% macro la_demographics(months_back) %}
-- View 6: Demographics breakdown by age band
-- AccessAva only: other sources have NULL AGE_BAND in stg_la_queries
-- Note: age field is <5% populated - expect high 'Unknown' rates (known PoC limitation)
-- Grain: one row per LA × age band within the time window
WITH base AS (
    SELECT
        LA_NAME,
        COALESCE(AGE_BAND, 'Unknown') AS AGE_BAND,
        SUM(QUERY_COUNT)              AS RAW_COUNT
    FROM {{ ref('stg_la_queries') }}
    WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
      AND SOURCE_SYSTEM = 'AccessAva'
    GROUP BY 1, 2
)
SELECT
    LA_NAME,
    AGE_BAND,
    {{ la_suppress('RAW_COUNT') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}              AS TIME_WINDOW_MONTHS
FROM base
{% endmacro %}
