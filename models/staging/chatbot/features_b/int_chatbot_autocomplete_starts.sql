{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_autocomplete_starts (R funcs.R). TRUE for a
  transcript whose tracker (R: static tr_autocomplete_starts.fst, here live
  from stg_chatbot_transcript) mentions 'Let me help you write a letter or
  email', or that has an event whose text contains 'Let me help you write a
  letter'. One row per transcript.
*/

with tr as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript') }}
    where raw_tracker_record like '%Let me help you write a letter or email%'
),

ev as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript_event') }}
    where text like '%Let me help you write a letter%'
)

select transcript_id, true as autocomplete_start
from (
    select transcript_id from tr
    union
    select transcript_id from ev
)
