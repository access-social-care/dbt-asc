{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_ethnicity(). The answer given right after the
  Contentful question "ZZZ - Ethnicity type" (hsbCdljYn4jPeiKSkLBzw).
  Skip/back answers dropped, trimmed, de-duplicated (R: unique()).
*/

select distinct transcript_id, {{ chatbot_str_trim('text') }} as ethnicity
from ({{ chatbot_answers_after("'hsbCdljYn4jPeiKSkLBzw'") }})
where text <> 'Skip this question' and text <> 'back'
