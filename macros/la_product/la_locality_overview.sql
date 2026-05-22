{% macro la_locality_overview(months_back, source_model='stg_la_queries') %}
-- View 1: Localities overview
-- Grain: one row per LA x locality within the time window
-- Both sources: AdvicePro locality = ward (from postcode lookup).
--               AccessAva locality = NULL pending sub-LA field confirmation in ACCESSAVA_LOCALITY.
SELECT
    LA_NAME,
    LOCALITY_NAME,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS QUERY_COUNT_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
GROUP BY 1, 2
{% endmacro %}
