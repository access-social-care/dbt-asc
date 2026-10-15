{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_person_type(). Answer after the Contentful question
  "ZZZ - equalities person" (3Q896ffuUkFEi4OJa516Ag). R sorts events by
  created_at DESCENDING before taking lag(), so its "previous" row is the next
  event in time; mirrored here (order by created_at desc, id). Then, if the
  user backed up and chose again, the first row in that descending order (the
  latest-listed) is kept.
*/

select transcript_id, person_type
from (
    select
        transcript_id,
        {{ chatbot_str_trim('text') }} as person_type,
        row_number() over (partition by transcript_id order by created_at desc, id) as rn
    from ({{ chatbot_answers_after("'3Q896ffuUkFEi4OJa516Ag'", order_by='created_at desc, id') }})
    where text <> 'Skip this question' and text <> 'back'
)
where rn = 1
