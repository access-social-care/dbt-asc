{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_ethnicity(). The answer given right after the
  Contentful question "ZZZ - Ethnicity type" (hsbCdljYn4jPeiKSkLBzw).
  Skip/back answers dropped, trimmed.

  Deliberate difference from R: R returns one row per distinct answer, so a
  user who backed up and chose again appears twice; ACCESSAVA's dedupe_by_id
  then keeps the last row. The key is made unique here the same way (latest
  answer by event id wins).
*/

select transcript_id, ethnicity
from (
    select
        transcript_id,
        {{ chatbot_str_trim('text') }} as ethnicity,
        row_number() over (partition by transcript_id order by id desc) as rn
    from ({{ chatbot_answers_after("'hsbCdljYn4jPeiKSkLBzw'") }})
    where text <> 'Skip this question' and text <> 'back'
)
where rn = 1
