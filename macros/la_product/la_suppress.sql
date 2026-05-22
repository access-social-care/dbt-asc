{% macro la_suppress(count_expr) %}
CASE
    WHEN {{ count_expr }} < 5 THEN '1-5'
    ELSE CAST({{ count_expr }} AS VARCHAR)
END
{% endmacro %}
