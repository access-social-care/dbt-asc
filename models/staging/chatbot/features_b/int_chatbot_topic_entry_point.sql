{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_topic_entry_point (R funcs.R). Every event whose
  Contentful entry has a t_topicentrypoint contributes that topic; per
  transcript the distinct topics in first-seen (event id) order joined with
  '; ' (R: paste(collapse = "; ")). No Source filter, as in R. The 'start'
  and 'startbrowse' pseudo-topics are kept here (CHARTER: excluded only in
  the topic chart).
*/

with contentful as (
    select distinct id, t_topicentrypoint
    from {{ ref('stg_contentful_data') }}
    where t_topicentrypoint is not null
),

hits as (
    select e.transcript_id, c.t_topicentrypoint as topic, min(e.id) as first_id
    from {{ ref('stg_chatbot_transcript_event') }} e
    join contentful c on e.contentful_id = c.id
    group by 1, 2
)

select
    transcript_id,
    string_agg(topic, '; ' order by first_id, topic) as topic_entry_point
from hits
group by 1
