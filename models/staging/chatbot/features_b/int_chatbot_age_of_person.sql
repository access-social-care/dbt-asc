{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_age_of_person (R funcs.R). Output column age.

  After an event with one of the three age-question Contentful ids, the next
  two events in the transcript (event id order) hold the answer. R keeps a row
  only when both lead(text, 1) and lead(text, 2) exist and neither is 'back'
  (a missing value makes the != test NA, which filter() drops), then maps the
  answer text to '18-64', 'Under 18' or '65+' (first matching case wins) and
  drops unmapped rows. With several answers the R rule keeps the LAST of the
  distinct values in first-seen order (gsub(".+;", "") on the joined string),
  mirrored here with arg_max over each value's first event id.
*/

with led as (
    select
        transcript_id,
        id,
        contentful_id,
        lead(text, 1) over (partition by transcript_id order by id) as a1,
        lead(text, 2) over (partition by transcript_id order by id) as a2
    from {{ ref('stg_chatbot_transcript_event') }}
),

answers as (
    select
        transcript_id,
        id,
        case
            when a1 = '18 and above' or a1 = 'I''m 18 or older' or a2 = '18 and over' then '18-64'
            when a1 = 'I''m younger than 18' or a1 = 'I''m younger than 18 '
                 or a1 = 'Under 18' or a2 = 'Under 18' then 'Under 18'
            when a1 = 'I am 65 or older' or a1 = 'They are 65 or older'
                 or a2 = 'They are 65 or older' then '65+'
        end as age
    from led
    where contentful_id in ('UaIuNrIdOdXjtG9yMO7M5', '53Ydzs5chOdnr2Q3QEOs6M', '3dRd4zw6iuKsVx2PHcU5Gp')
      and a1 <> 'back' and a2 <> 'back'
),

firsts as (
    select transcript_id, age, min(id) as first_id
    from answers
    where age is not null
    group by 1, 2
)

select transcript_id, arg_max(age, first_id) as age
from firsts
group by 1
