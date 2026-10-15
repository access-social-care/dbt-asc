{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_age_group(). The answer right after the Contentful
  question "ZZZ - your age" (2nwxcbTWEMyR1yitCWVM8e), trimmed, 'back' dropped,
  de-duplicated. Output column is age_range (as R).
*/

select distinct transcript_id, {{ chatbot_str_trim('text') }} as age_range
from ({{ chatbot_answers_after("'2nwxcbTWEMyR1yitCWVM8e'") }})
where {{ chatbot_str_trim('text') }} <> 'back'
