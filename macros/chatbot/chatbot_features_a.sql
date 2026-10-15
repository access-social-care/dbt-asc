{#
  Helpers for the chatbot_* feature models (group A): int_chatbot_*.
  The model rules are ported from ascFuncs/R/funcs.R; one model per function.
#}

{# stringr::str_trim: strips leading/trailing Unicode White_Space (stringi), which
   includes the non-breaking space that RE2 \s does not match. #}
{% macro chatbot_str_trim(col) -%}
    regexp_replace({{ col }}, '^[\s\p{Z}\x{0B}\x{85}]+|[\s\p{Z}\x{0B}\x{85}]+$', '', 'g')
{%- endmacro %}

{# Answer to a demographics question: the text of the event that comes right
   after a bot message whose Contentful id is `question_ids` (R:
   lag(contentful_id, 1) %in% ids within transcript_id, events in id order).
   Returns transcript_id, text, id, created_at of the answer events. Empty
   answers ('') are dropped: R returns them, but ACCESSAVA reads data.csv back
   with '' as NA, and dedupe_by_id then prefers any non-NA row, so '' never
   survives into ACCESSAVA. #}
{% macro chatbot_answers_after(question_ids, order_by='id') -%}
    select transcript_id, id, created_at, text
    from (
        select
            transcript_id, id, created_at, text,
            lag(contentful_id, 1) over (partition by transcript_id order by {{ order_by }}) as prev_contentful_id
        from {{ ref('stg_chatbot_transcript_event') }}
    )
    where prev_contentful_id in ({{ question_ids }}) and text <> ''
{%- endmacro %}

{# Generic test: model has exactly one row per distinct transcript_id of the
   base model (optionally restricted by a where on the base). #}
{% test chatbot_rows_match_base(model, base_model, base_where='1=1') %}
    select 1 as failure
    from (
        select
            (select count(*) from {{ model }}) as n_model,
            (select count(distinct transcript_id) from {{ base_model }} where {{ base_where }}) as n_base
    )
    where n_model <> n_base
{% endtest %}

{# Generic test: every transcript_id of the model exists in the base model. #}
{% test chatbot_ids_in_base(model, column_name, base_model) %}
    select m.{{ column_name }}
    from {{ model }} m
    left join (select distinct transcript_id from {{ base_model }}) b on m.{{ column_name }} = b.transcript_id
    where b.transcript_id is null
{% endtest %}
