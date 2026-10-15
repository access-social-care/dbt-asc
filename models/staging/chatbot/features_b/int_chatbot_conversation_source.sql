{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_conversation_source (R funcs.R). R queries
  actions_transcript directly (id, referring_page where not null) and
  tidyr::separate()s the value at the first '?' into url and parameters
  (extra = "merge", fill = "right": no '?' gives NULL parameters). Read here
  from stg_chatbot_transcript. One row per transcript with a referring page.
*/

select
    transcript_id,
    case when instr(referring_page, '?') > 0
         then substr(referring_page, 1, instr(referring_page, '?') - 1)
         else referring_page end as referring_page_url,
    case when instr(referring_page, '?') > 0
         then substr(referring_page, instr(referring_page, '?') + 1) end as referring_page_parameters
from {{ ref('stg_chatbot_transcript') }}
where referring_page is not null
