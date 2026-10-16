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

{#
  Bridge: port of ascFuncs make_bridge_dim(multi = TRUE) / split_to_long().
  One row per (transcript_id, value): split on the delimiter with optional
  whitespace around it (R: "\s*;\s*"), squish each token (trim and collapse
  whitespace runs, Unicode spaces included), drop NA and ''. No distinct, as in
  R, so a value repeated inside one list gives repeated rows.
  data_uploader.R passes delim = ";" for every attribute, so ';' splits the
  '; '-joined columns too. The value column keeps the ACCESSAVA column name.
#}
{% macro chatbot_bridge(value_col) -%}
select
    transcript_id,
    token as {{ value_col }}
from (
    select
        transcript_id,
        trim(regexp_replace(unnest(regexp_split_to_array({{ value_col }}, '\s*;\s*')), '[\s\p{Z}]+', ' ', 'g')) as token
    from {{ ref('accessava') }}
    where {{ value_col }} is not null
) t
where token <> ''
{%- endmacro %}

{#
  Dimension: make_bridge_dim()$dim + add_surrogate_key(). Distinct bridge
  values sorted in binary (C-locale) order, which is what dplyr::arrange does
  on character vectors, with row_number() as the surrogate key.
#}
{% macro chatbot_dim(bridge_model, value_col, key_col) -%}
select
    cast(row_number() over (order by {{ value_col }}) as bigint) as {{ key_col }},
    {{ value_col }}
from (
    select distinct {{ value_col }} from {{ ref(bridge_model) }}
) d
{%- endmacro %}
