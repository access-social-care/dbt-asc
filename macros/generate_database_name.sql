{#
  Database-name resolution override.

  DuckDB/MotherDuck target: one database per layer, schema `main`.
  marts and normalised models land in `marts`; everything else (staging,
  intermediate, seeds, tests) in `staging`. `raw`, `raw_external` and
  `reference` are inputs and are never written by dbt.

  Any other target (Snowflake) returns EXACTLY dbt's default:
  custom_database_name if set, else target.database.
#}

{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- if target.type == 'duckdb' -%}
        {%- if node is not none and node.resource_type is defined and node.resource_type == 'model'
               and node.fqn | length > 2 and node.fqn[1] in ['marts', 'normalised'] -%}
            marts
        {%- else -%}
            staging
        {%- endif -%}
    {%- else -%}
        {%- set default_database = target.database -%}
        {%- if custom_database_name is none -%}
            {{ default_database }}
        {%- else -%}
            {{ custom_database_name }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
