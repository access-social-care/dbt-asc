{#
  Generic tests for the helplines layer. They replace the checks the R promote scripts
  ran before a table reached HELPLINES.PUBLIC (promote_helplines_staging.R: shape check by
  DESCRIBE TABLE name and type; the row-count guard in the promotion transaction).
#}

{# Shape check: the model has exactly the nine CANONICAL_PARTNER_COLS, in order, DATE then
   eight VARCHAR. A test returns rows when it fails. #}
{% test helplines_canonical_shape(model) %}
    {%- set expected = [
        ['date', 'DATE'], ['month', 'VARCHAR'], ['t1', 'VARCHAR'], ['t2', 'VARCHAR'],
        ['ut1', 'VARCHAR'], ['ut2', 'VARCHAR'], ['la_name', 'VARCHAR'], ['la_code', 'VARCHAR'],
        ['org', 'VARCHAR']
    ] -%}
    {%- set ns = namespace(bad=false, found=[]) -%}
    {%- if execute -%}
        {%- set cols = adapter.get_columns_in_relation(model) -%}
        {%- if cols | length != expected | length -%}{%- set ns.bad = true -%}{%- endif -%}
        {%- for c in cols -%}
            {%- do ns.found.append(c.name | lower ~ ' ' ~ c.dtype | upper) -%}
            {%- if loop.index0 < expected | length -%}
                {%- if c.name | lower != expected[loop.index0][0] or c.dtype | upper != expected[loop.index0][1] -%}{%- set ns.bad = true -%}{%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}
    select '{{ ns.found | join(", ") }}' as columns_found
    where {{ 'true' if ns.bad else 'false' }}
{% endtest %}

{# Row-count floor: fails when the model has fewer than min_rows rows. A floor, not an
   equality: the history only grows, so a drop means a source table vanished or a filter broke. #}
{% test helplines_min_rows(model, min_rows) %}
    select count(*) as n_rows
    from {{ model }}
    having count(*) < {{ min_rows }}
{% endtest %}

{# Every non-null value is an ONS-style code (one letter, eight digits). The literal NA is allowed: the Mencap/RNIB seed stores it and Snowflake holds it too. #}
{% test helplines_gss_code(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} <> 'NA'
      and not regexp_matches({{ column_name }}, '^[A-Z][0-9]{8}$')
{% endtest %}

{# stg_helplines reads the aggregated mart. Snowflake: the table loaded by helplines_data
   (load_to_snowflake.R). DuckDB: this project builds it from the Fivetran partner files. #}
{% macro helplines_aggregated_relation() -%}
    {%- if target.type == 'duckdb' -%}
        {{ ref('helplines_ongoing') }}
    {%- else -%}
        {{ source('helplines', 'helplines_aggregated_full') }}
    {%- endif -%}
{%- endmacro %}
