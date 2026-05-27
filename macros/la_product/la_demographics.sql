{% macro la_demographics(months_back, source_model='stg_la_queries', suppress=true) %}
-- View 6: Demographics breakdown by age band
-- Grain: one row per LA x age band within the time window
-- Note: AccessAva age field is <5% populated; AdvicePro has age_range.
--       Both sources — NULL AGE_BAND → 'Unknown'.
-- suppress=false: returns raw QUERY_COUNT_RAW for intermediate/BI use
SELECT
    LA_NAME,
    COALESCE(AGE_BAND, 'Unknown')         AS AGE_BAND,
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
