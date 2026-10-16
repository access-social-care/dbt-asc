{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_churn(). Two parts joined on transcript_id:
    churn_point    the text of the latest event (created_at, ties by id) whose
                   text is non-blank, not NULL/'NULL'/'REDACTED', with quotes,
                   newlines and literal backslash-n removed, trimmed.
    churn_category furthest journey stage reached, from CHATBOT_CHURN_START_IDS
                   / CHATBOT_CHURN_END_IDS (R/globals.R) and "Download letter"
                   / "Download Document" texts.
  Only transcripts with a churn_point are output (R left-joins the category
  onto the churn points).
*/

with churn_points as (
    select
        transcript_id,
        {{ chatbot_str_trim("regexp_replace(text, '\\\\?\"|\\n|\\\\+n', '', 'g')") }} as churn_point
    from {{ ref('stg_chatbot_transcript_event') }}
    where text <> '' and text <> 'NULL' and text <> 'REDACTED'
    qualify row_number() over (partition by transcript_id order by created_at desc, id) = 1
),

flags as (
    select
        transcript_id,
        max(coalesce(contentful_id in (
            '15qOdA7aWNqNuT8rLpHFrB', '2t3EGD48YxAapTvmcDMWL5', 'dJX5nr5vuzi5tTKH9HK4X',
            '7I2y71CLioslzejZZsz7a6', '7wsb12PZm8FvxDY4UJsg1P', '346XvTkjjQqgnI5QQNxglR'
        ), false)::int) as reached_topic_list,
        max((coalesce(contains(text, 'Download letter'), false)
             or coalesce(contains(text, 'Download Document'), false))::int) as got_to_letter,
        max(coalesce(contentful_id in ('5DALsDX61AYEI5wUXrtgdQ', '7kAURNTMUeMdr0o1lnnzp4'), false)::int) as stage_end
    from {{ ref('stg_chatbot_transcript_event') }}
    group by transcript_id
),

categories as (
    select
        transcript_id,
        case
            when stage_end = 1 and got_to_letter = 0 then '5 end, no letter'
            when stage_end = 1 then '4 got to feedback'
            when got_to_letter = 1 then '3 got to letter'
            when reached_topic_list = 1 then '2 reached topic list'
            else '1 before topic'
        end as churn_category
    from flags
)

select c.transcript_id, c.churn_point, k.churn_category
from churn_points c
left join categories k using (transcript_id)
