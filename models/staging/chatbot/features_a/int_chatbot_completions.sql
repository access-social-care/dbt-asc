{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_completions(). One row per transcript that reached a
  completion point; completed is always TRUE.

  Completion points are looked up from Contentful by name (3B ... download
  letter, 1 safeguarding 3f), as R does. Two sources, unioned:
    1. an event whose text is one of the completion texts (exact match);
    2. a transcript whose raw_tracker_record matches the completion texts
       joined with '|' as a regex (extract_tr_data.R tr_completions.fst). The
       safeguarding text contains '$' and a newline, so in practice only
       'Download letter' matches, in R and here.
*/

with completion_texts as (
    select text
    from {{ source('chatbot_landing', 'contentful_data') }}
    where regexp_matches(name, '3B -which option suits you - download letter|1 safeguarding 3f')
),

pattern as (
    select string_agg(text, '|') as p from completion_texts
),

from_events as (
    select distinct transcript_id
    from {{ ref('stg_chatbot_transcript_event') }}
    where text in (select text from completion_texts)
),

from_tracker as (
    select distinct t.transcript_id
    from {{ ref('stg_chatbot_transcript') }} t, pattern
    where t.is_engaged and regexp_matches(t.raw_tracker_record, pattern.p)
)

select transcript_id, true as completed
from (
    select transcript_id from from_events
    union
    select transcript_id from from_tracker
)
