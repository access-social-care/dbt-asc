{% macro la_legal_letters(months_back, source_model='stg_la_queries') %}
-- View 7: Legal letters - total queries vs letters generated
-- AccessAva only: only source with HAS_LETTER dimension in stg_la_queries
-- Grain: one row per LA for the full time window
SELECT
    LA_NAME,
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS TOTAL_QUERIES_DISPLAY,
    {{ la_suppress('SUM(HAS_LETTER)') }}  AS TOTAL_LETTERS_DISPLAY,
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
  AND SOURCE_SYSTEM = 'AccessAva'
GROUP BY 1
{% endmacro %}
