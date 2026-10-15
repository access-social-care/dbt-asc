{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_autocomplete_finish (R funcs.R). TRUE for a
  transcript whose tracker matches 'Thank you for answering the questions
  $UserName.' (R regex, final . is any character), or that has an event with
  contentful_id 5ZZcbhzx2q0Z51xPXSEhvg (download filled template). One row per
  transcript.
*/

with tr as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript') }}
    where regexp_matches(raw_tracker_record, 'Thank you for answering the questions \$UserName.')
),

ev as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript_event') }}
    where contentful_id = '5ZZcbhzx2q0Z51xPXSEhvg'
)

select transcript_id, true as autocomplete_finish
from (
    select transcript_id from tr
    union
    select transcript_id from ev
)
