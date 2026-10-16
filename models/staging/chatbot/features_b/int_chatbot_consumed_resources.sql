{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_consumed_asc_resources (R funcs.R). Output column
  consumed_resources (R name).

  Within a transcript (events ordered by created_at, then id as R's stable
  arrange), each event takes the topic entry point of the most recent event
  that had one (tidyr::fill, so system messages inherit the topic). Bot
  messages containing '|' are resource links: text is cut at the first ' |'
  and then at the first '_' (R: gsub(" \|.+", "") then gsub("_.+", "")).
  Distinct (resource, topic) pairs per transcript in first-seen order, each
  rendered 'resource|topic' (a missing topic renders as the text 'NA', as R's
  tidyr::unite does) and joined with ';'.
*/

with contentful as (
    select distinct id, t_topicentrypoint as topic
    from {{ source('chatbot_raw', 'contentful_data') }}
    where t_topicentrypoint is not null
),

filled as (
    select
        e.transcript_id,
        e.created_at,
        e.id,
        e.bot_or_user,
        e.text,
        last_value(c.topic ignore nulls) over (
            partition by e.transcript_id order by e.created_at, e.id
            rows between unbounded preceding and current row) as topic
    from {{ ref('stg_chatbot_transcript_event') }} e
    left join contentful c on e.contentful_id = c.id
),

resources as (
    select
        transcript_id,
        regexp_replace(regexp_replace(text, '(?s) \|.+', ''), '(?s)_.+', '') as resource,
        topic,
        row_number() over (partition by transcript_id order by created_at, id) as rn
    from filled
    where bot_or_user = 'bot' and text like '%|%'
),

firsts as (
    select transcript_id, resource, topic, min(rn) as first_rn
    from resources
    group by 1, 2, 3
)

select
    transcript_id,
    string_agg(resource || '|' || coalesce(topic, 'NA'), ';' order by first_rn) as consumed_resources
from firsts
group by 1
