{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_armed_forces(). Answer right after the Contentful
  question "ZZZ - final question" (4501nQWffIkFg5lfybhlsA), trimmed, Skip
  dropped. If the user backed up and answered again, R keeps the first row in
  event (id) order after unique(); mirrored here.
*/

select transcript_id, armed_service
from (
    select
        transcript_id,
        {{ chatbot_str_trim('text') }} as armed_service,
        row_number() over (partition by transcript_id order by id) as rn
    from ({{ chatbot_answers_after("'4501nQWffIkFg5lfybhlsA'") }})
    where {{ chatbot_str_trim('text') }} <> 'Skip this question'
)
where rn = 1
