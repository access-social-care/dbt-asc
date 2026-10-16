{{ config(tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Test-user removal held: no surviving transcript's tracker answer, and no
-- event following "What should I call you?", is a test name. Returns offending
-- ids (= failure).

select 'stg_chatbot_transcript' as model_name, id as offending_id
from {{ ref('stg_chatbot_transcript') }}
where {{ chatbot_tracker_name_answer('raw_tracker_record') }} in {{ chatbot_test_users() }}

union all

select 'stg_chatbot_transcript_event', transcript_id
from (
    select
        transcript_id,
        lower(text) as text_lower,
        lag(text, 1) over (partition by transcript_id order by id) as prev_text
    from {{ ref('stg_chatbot_transcript_event') }}
)
where coalesce(regexp_matches(prev_text, 'What should I call you?'), false)
  and text_lower in {{ chatbot_test_users() }}
