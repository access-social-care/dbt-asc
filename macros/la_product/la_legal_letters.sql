{% macro la_legal_letters(months_back, source_model='stg_la_topic_mentions_glos', suppress=true) %}
-- View 7: Legal letters - total queries vs letters generated
-- AccessAva only: only source with HAS_LETTER dimension in stg_la_topic_mentions
-- Grain: one row per LA for the full time window
-- suppress=false: returns raw QUERY_COUNT_RAW / HAS_LETTER_RAW for intermediate/BI use
SELECT
    LA_NAME,
    {% if suppress %}
    {{ la_suppress('SUM(QUERY_COUNT)') }} AS TOTAL_QUERIES_DISPLAY,
    {{ la_suppress('SUM(HAS_LETTER)') }}  AS TOTAL_LETTERS_DISPLAY,
    {% else %}
    SUM(QUERY_COUNT) AS QUERY_COUNT_RAW,
    SUM(HAS_LETTER)  AS HAS_LETTER_RAW,
    {% endif %}
    {{ months_back }}                     AS TIME_WINDOW_MONTHS
FROM {{ ref(source_model) }}
WHERE QUERY_DATE >= DATEADD('month', -{{ months_back }}, CURRENT_DATE())
  AND SOURCE_SYSTEM = 'AccessAva'
GROUP BY 1
{% endmacro %}
