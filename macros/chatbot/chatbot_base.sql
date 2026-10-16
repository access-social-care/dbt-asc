{#
  Chatbot base-layer helpers (DuckDB/MotherDuck only; the chatbot models are
  disabled on the Snowflake targets). Ported from ascFuncs/R/funcs.R
  (chatbot_process_tr, chatbot_process_tr_ev). One macro per distinct
  extraction pattern; feature models reuse these rather than re-writing them.
#}

{# Test-user first names removed from both tr and tr_ev
   (funcs.R: test_users <- c("miriam", "fin", "test", "kari", "testing")). #}
{% macro chatbot_test_users() -%}
    ('miriam', 'fin', 'test', 'kari', 'testing')
{%- endmacro %}

{# R as.Date(created_at): POSIXct from RPostgres is UTC, so the day is the UTC
   day, not the MotherDuck session-time-zone day. #}
{% macro chatbot_entry_day(ts_col) -%}
    CAST({{ ts_col }} AT TIME ZONE 'UTC' AS DATE)
{%- endmacro %}

{# 'YYYY-MM', zero padded (funcs.R entry_month). #}
{% macro chatbot_entry_month(day_col) -%}
    (CAST(year({{ day_col }}) AS VARCHAR) || '-' || lpad(CAST(month({{ day_col }}) AS VARCHAR), 2, '0'))
{%- endmacro %}

{# 'YYYY-WW' where WW = lubridate::week() = (day_of_year - 1) // 7 + 1, with the
   calendar year (not the ISO year), zero padded (funcs.R entry_week). #}
{% macro chatbot_entry_week(day_col) -%}
    (CAST(year({{ day_col }}) AS VARCHAR) || '-'
     || lpad(CAST(((dayofyear({{ day_col }}) - 1) // 7) + 1 AS VARCHAR), 2, '0'))
{%- endmacro %}

{# Scalar string at a JSON path in a VARCHAR JSON column, e.g.
   chatbot_json_string('raw_record', '$.cms_response.id'). NULL when the path is
   absent or JSON null. (R: jqr::jq(raw_record, ".cms_response.id") returned the
   text "null" for absent paths; here it is a true NULL.) #}
{% macro chatbot_json_string(col, path) -%}
    json_extract_string({{ col }}, '{{ path }}')
{%- endmacro %}

{# The answer to "What should I call you?" as chatbot_process_tr reads it from
   raw_tracker_record: second `user_input...}` match (the first if only one
   match), text from the first ':' to the last ',' in it, letters only,
   lower-cased. Regex-based on purpose: this is how the R rule works and it must
   stay byte-compatible for parity. NULL/'' when there is no usable match. #}
{% macro chatbot_tracker_name_answer(col) -%}
    lower(regexp_replace(
        regexp_extract(
            list_extract(
                regexp_extract_all({{ col }}, 'user_input.+?\}'),
                least(2, len(regexp_extract_all({{ col }}, 'user_input.+?\}')))),
            ':.+,'),
        '[^a-zA-Z]', '', 'g'))
{%- endmacro %}

{# report.Rmd (the job that renders the report AND exports data.csv, which
   data_uploader.R loads into AVA.PUBLIC.ACCESSAVA) drops tenants whose name
   contains UAT or APDAS_SQUARESPACE. process_raw_data itself keeps them. Base
   models expose this as a flag instead of dropping rows, so the base stays
   equal to the R tr / tr_ev output and ACCESSAVA parity filters on it. #}
{% macro chatbot_is_reporting_tenant(col) -%}
    (NOT {{ col }} LIKE '%UAT%' AND NOT {{ col }} LIKE '%APDAS_SQUARESPACE%')
{%- endmacro %}

{# report.Rmd tenant tidy-up: strip _prod/_Prod, underscores to spaces. A no-op
   on today's organisation_name values (they are already clean); kept so the
   final ACCESSAVA-shaped model applies exactly the report.Rmd rule. #}
{% macro chatbot_tenant_report_name(col) -%}
    replace(replace(replace({{ col }}, '_prod', ''), '_Prod', ''), '_', ' ')
{%- endmacro %}
