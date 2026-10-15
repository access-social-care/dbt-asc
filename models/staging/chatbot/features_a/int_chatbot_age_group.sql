{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_age_group(). The answer right after the Contentful
  question "ZZZ - your age" (2nwxcbTWEMyR1yitCWVM8e), trimmed, 'back' dropped
  ('Skip this question' is kept, as in R). Output column is age_range (as R).

  Deliberate difference from R: R returns one row per distinct answer, so a
  user who backed up and chose again appears twice; ACCESSAVA's dedupe_by_id
  then keeps the last row. The key is made unique here the same way (latest
  answer by event id wins).
*/

select transcript_id, age_range
from (
    select
        transcript_id,
        {{ chatbot_str_trim('text') }} as age_range,
        row_number() over (partition by transcript_id order by id desc) as rn
    from ({{ chatbot_answers_after("'2nwxcbTWEMyR1yitCWVM8e'") }})
    where {{ chatbot_str_trim('text') }} <> 'back'
)
where rn = 1
