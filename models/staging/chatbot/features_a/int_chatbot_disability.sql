{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_disability(). Answers right after the Contentful
  questions "ZZZ - disability question" (Yes/No, 48leJ88k96NV3DjyXE0UGz) and
  "ZZZ - disability types" (multi-select conditions, rFmdqP3nA36j2ys1J6Uzg).
  Skip/back dropped, trimmed; a Yes/No answer goes to disability_n_y, anything
  else to disability_text. Several distinct answers per transcript are
  concatenated with ';' in sorted order (R: paste(sort(unique(.)), collapse)).
*/

with answers as (
    select
        transcript_id,
        {{ chatbot_str_trim('text') }} as disability
    from ({{ chatbot_answers_after("'48leJ88k96NV3DjyXE0UGz', 'rFmdqP3nA36j2ys1J6Uzg'") }})
    where text <> 'Skip this question' and text <> 'back'
)

select
    transcript_id,
    string_agg(distinct disability, ';' order by disability) filter (where disability in ('Yes', 'No')) as disability_n_y,
    string_agg(distinct disability, ';' order by disability) filter (where disability not in ('Yes', 'No')) as disability_text
from answers
group by transcript_id
