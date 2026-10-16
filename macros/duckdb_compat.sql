{#
  Cross-adapter helpers for the DuckDB (MotherDuck) target. Each macro renders
  the original Snowflake SQL when target.type is not 'duckdb', so the prod
  path is unchanged.
#}

{# One row per delimiter-separated item of `col`. Returns a SELECT of
   keep_cols plus out_col (TRIM-med item). outer=true keeps a NULL-input row
   as a single NULL item (Snowflake FLATTEN OUTER => TRUE). #}
{% macro split_to_rows(relation, col, delimiter, keep_cols, out_col, outer=false) -%}
    {%- if target.type == 'duckdb' -%}
    SELECT
        {% for c in keep_cols %}{{ c }},
        {% endfor %}TRIM(_item) AS {{ out_col }}
    FROM (
        SELECT
            {% for c in keep_cols %}{{ c }},
            {% endfor %}UNNEST(
                {%- if outer %}COALESCE(STRING_SPLIT({{ col }}, '{{ delimiter }}'), [CAST(NULL AS VARCHAR)]){%- else %}STRING_SPLIT({{ col }}, '{{ delimiter }}'){%- endif %}
            ) AS _item
        FROM {{ relation }}
    )
    {%- else -%}
    SELECT
        {% for c in keep_cols %}{{ c }},
        {% endfor %}TRIM(f.value::VARCHAR) AS {{ out_col }}
    FROM {{ relation }},
    LATERAL FLATTEN(
        INPUT  => SPLIT({{ col }}, '{{ delimiter }}'){% if outer %},
        OUTER  => TRUE{% endif %}
    ) f
    {%- endif -%}
{%- endmacro %}
