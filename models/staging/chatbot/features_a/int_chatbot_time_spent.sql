{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_time_spent(). dif = seconds between the first and
  the last event of a transcript, where first/last are by event id (not time),
  rounded to whole seconds (R round(): half to even).
*/

select
    transcript_id,
    round_even(epoch(arg_max(created_at, id)) - epoch(arg_min(created_at, id)), 0) as dif
from {{ ref('stg_chatbot_transcript_event') }}
group by transcript_id
