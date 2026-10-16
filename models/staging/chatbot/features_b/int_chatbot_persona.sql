{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_persona (R funcs.R). One row per transcript that
  has at least one Source = true event whose Contentful entry carries a
  persona. persona is the distinct persona codes in first-seen (event id)
  order joined with '; ' (R: paste(collapse = "; ")), e.g. '1; 3'. A
  conversation may therefore hold several personas (CHARTER: multi-valued,
  bridge/dim built later). Codes are Contentful t_persona (1, 2, 3).
*/

with contentful as (
    select distinct id, cast(t_persona as bigint) as t_persona
    from {{ source('chatbot_raw', 'contentful_data') }}
    where t_persona is not null
),

hits as (
    select e.transcript_id, cast(c.t_persona as varchar) as persona, min(e.id) as first_id
    from {{ ref('stg_chatbot_transcript_event') }} e
    join contentful c on e.contentful_id = c.id
    where e.tags like '%"Source": "true"%'
    group by 1, 2
)

select
    transcript_id,
    string_agg(persona, '; ' order by first_id, persona) as persona
from hits
group by 1
