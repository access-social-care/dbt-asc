{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_letters_downloaded (R funcs.R). A transcript is a
  letter download when its tracker mentions "Download letter" (R read this
  from the static tr_letters.fst extracted from raw_tracker_record; here it
  is read live from stg_chatbot_transcript) or an event has text
  "Download letter". lettercode: the lettercode in force at each such event
  (last non-null lettercode so far in the transcript, R: tidyr::fill),
  joined with ';' in event order, duplicates kept; unknown codes render as
  the text 'NA' and a value that is only 'NA' becomes NULL (R). Tracker-only
  transcripts have NULL lettercode.
*/

with ev as (
    select
        transcript_id,
        id,
        text,
        last_value(lettercode ignore nulls) over (
            partition by transcript_id order by id
            rows between unbounded preceding and current row) as filled_lettercode
    from {{ ref('stg_chatbot_transcript_event') }}
),

ev_letters as (
    select
        transcript_id,
        string_agg(coalesce(filled_lettercode, 'NA'), ';' order by id) as lettercode
    from ev
    where text = 'Download letter'
    group by 1
),

tr_letters as (
    select transcript_id
    from {{ ref('stg_chatbot_transcript') }}
    where raw_tracker_record like '%Download letter%'
)

select
    coalesce(t.transcript_id, e.transcript_id) as transcript_id,
    case when e.lettercode = 'NA' then null else e.lettercode end as lettercode,
    true as letter_downloaded
from tr_letters t
full join ev_letters e on t.transcript_id = e.transcript_id
