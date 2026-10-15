{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_conversation_source (R funcs.R). R queries
  actions_transcript directly (id, referring_page where not null, no test-user
  filtering) and tidyr::separate()s the value at the first '?' into url and
  parameters (extra = "merge", fill = "right": no '?' gives NULL parameters).
  Read from the source table, not stg_chatbot_transcript, for the same
  reason: transcripts the base drops (test users, no tenant) still get a row,
  as in R. Empty strings become NULL, as in ACCESSAVA after the R pipeline's
  data.csv round trip. One row per transcript with a referring page.
*/

with src as (
    select id, referring_page, _fivetran_deleted
    from {{ source('chatbot_postgres', 'actions_transcript') }}
    qualify row_number() over (partition by id order by _fivetran_synced desc nulls last) = 1
)

select
    id as transcript_id,
    nullif(case when instr(referring_page, '?') > 0
                then substr(referring_page, 1, instr(referring_page, '?') - 1)
                else referring_page end, '') as referring_page_url,
    nullif(case when instr(referring_page, '?') > 0
                then substr(referring_page, instr(referring_page, '?') + 1) end, '') as referring_page_parameters
from src
where referring_page is not null and not coalesce(_fivetran_deleted, false)
