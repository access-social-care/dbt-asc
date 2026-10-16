{#
  Relation that downstream models read AdvicePro casework tables from.
  Snowflake: the loaded table CASEWORK.PUBLIC.<name> (source `casework`, written
  by case_data/load_advicepro_to_snowflake.R). DuckDB: the dbt model of the
  same name, which replaces that loader's transform half (the extract half
  lands raw.advicepro.cases_raw).
  name: 'advicepro_casework' or 'case_topic_bridge'. advicepro_demographics
  and casework_locality have no MotherDuck equivalent and stay on source().
#}
{% macro casework_relation(name) -%}
    {%- if target.type == 'duckdb' -%}
        {{ ref(name) }}
    {%- else -%}
        {{ source('casework', name) }}
    {%- endif -%}
{%- endmacro %}

{#
  AdvicePro cleaning rule (loader: na_if(x, "") and na_if(x, "[Not Specified]")
  on every character column): blank and the placeholder both become NULL.
  Exact match, no trimming, as in the loader.
#}
{% macro advicepro_clean(col) -%}
    nullif(nullif({{ col }}, ''), '[Not Specified]')
{%- endmacro %}

{#
  Bridge: port of ascFuncs make_bridge_dim(multi = TRUE) / split_to_long().
  One row per (case_reference, value): split on the delimiter with optional
  whitespace around it (R: "\\s*;\\s*"), squish each token (trim and collapse
  whitespace runs), drop NULL and ''. No distinct, as in R.
#}
{% macro casework_bridge(relation, value_col) -%}
select
    case_reference,
    token as {{ value_col }}
from (
    select
        case_reference,
        trim(regexp_replace(
            unnest(regexp_split_to_array({{ value_col }}, '\s*;\s*')),
            '[\s\p{Z}]+', ' ', 'g')) as token
    from {{ relation }}
    where {{ value_col }} is not null
) t
where token <> ''
{%- endmacro %}

{#
  Dimension: make_bridge_dim()$dim + add_surrogate_key(). Distinct bridge
  values, surrogate key = row_number over the value in binary (C-locale)
  order, which is what dplyr::arrange does on character vectors.
#}
{% macro casework_dim(bridge_model, value_col, key_col) -%}
select
    cast(row_number() over (order by {{ value_col }}) as bigint) as {{ key_col }},
    {{ value_col }}
from (
    select distinct {{ value_col }} from {{ ref(bridge_model) }}
) d
{%- endmacro %}
