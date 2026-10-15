{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_factsheets_templates (R funcs.R). TRUE for a
  transcript whose tracker mentions 'Factsheets & Template letters', or that
  has an event whose text is exactly 'Factsheets & Template letters'. One row
  per transcript.
*/

with tr as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript') }}
    where raw_tracker_record like '%Factsheets & Template letters%'
),

ev as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript_event') }}
    where text = 'Factsheets & Template letters'
)

select transcript_id, true as factsheets_templates
from (
    select transcript_id from tr
    union
    select transcript_id from ev
)
