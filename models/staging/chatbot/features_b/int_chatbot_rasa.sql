{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_rasa (R funcs.R). TRUE for a transcript whose
  tracker mentions 'Ask a specific question', or that has an event whose text
  contains it (the user started the Rasa free-text question feature). One row
  per transcript.
*/

with tr as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript') }}
    where raw_tracker_record like '%Ask a specific question%'
),

ev as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript_event') }}
    where text like '%Ask a specific question%'
)

select transcript_id, true as rasa
from (
    select transcript_id from tr
    union
    select transcript_id from ev
)
