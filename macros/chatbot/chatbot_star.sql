{#
  Relation that downstream models read AccessAva conversations from.
  Snowflake: the loaded table AVA.PUBLIC.<name> (source `accessava`, written by
  chatbot_data/data_uploader.R). DuckDB: the dbt model of the same name, which
  replaces data.csv, report.Rmd's export step and data_uploader.R.
  name: 'accessava' or 'accessava_locality'.
#}
{% macro accessava_relation(name) -%}
    {%- if target.type == 'duckdb' -%}
        {{ ref(name) }}
    {%- else -%}
        {{ source('accessava', name) }}
    {%- endif -%}
{%- endmacro %}
