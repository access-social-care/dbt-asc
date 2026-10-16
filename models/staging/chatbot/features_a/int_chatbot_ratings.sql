{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_ratings(). Events whose text is Poor / Okay / Great
  (any case) map to 1 / 2 / 3; rating is the mean per transcript.
*/

select
    transcript_id,
    avg(case lower(text) when 'poor' then 1 when 'okay' then 2 when 'great' then 3 end) as rating
from {{ ref('stg_chatbot_transcript_event') }}
where lower(text) in ('poor', 'okay', 'great')
group by transcript_id
