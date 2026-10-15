{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_feedback(). Free-text feedback: the event after the
  bot message "Please type in your feedback below", or any event whose
  raw_record mentions user_feedback. 'back' dropped; several answers joined
  with ' |||ALSO||| '.

  R quirk mirrored: the "same text as the previous row" filter runs AFTER
  ungroup(), so the previous row is the previous qualifying event in global id
  order (any transcript), not within the transcript. See
  docs/parity/chatbot_features_a.md.
*/

with ev as (
    select
        transcript_id, id, text, raw_record,
        lag(text, 1) over (partition by transcript_id order by id) as prev_text
    from {{ ref('stg_chatbot_transcript_event') }}
),

picked as (
    select
        transcript_id, id, text,
        lag(lower(text), 1) over (order by id) as prev_lower
    from ev
    where coalesce(contains(prev_text, 'Please type in your feedback below'), false)
       or coalesce(contains(raw_record, 'user_feedback'), false)
)

select
    transcript_id,
    string_agg(text, ' |||ALSO||| ' order by id) as user_feedback
from picked
where lower(text) <> prev_lower and text <> 'back'
group by transcript_id
