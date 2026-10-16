{{ config(tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- entry_month / entry_week are well-formed and agree with entry_day; the
-- tenant exclusions hold. Returns offending rows (= failure).

select 'stg_chatbot_transcript' as model_name, id::varchar as offending_id
from {{ ref('stg_chatbot_transcript') }}
where not regexp_full_match(entry_month, '[0-9]{4}-[0-9]{2}')
   or not regexp_full_match(entry_week, '[0-9]{4}-[0-9]{2}')
   or entry_month <> strftime(entry_day, '%Y-%m')
   or tenant_name = 'AccessAva #test'

union all

select 'stg_chatbot_transcript_event', id::varchar
from {{ ref('stg_chatbot_transcript_event') }}
where not regexp_full_match(entry_month, '[0-9]{4}-[0-9]{2}')
   or not regexp_full_match(entry_week, '[0-9]{4}-[0-9]{2}')
   or entry_month <> strftime(entry_day, '%Y-%m')
   or tenant_name = 'AccessAva #test'
