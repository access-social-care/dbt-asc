{{ config(tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Row-count reasonableness: each base model keeps between 90% and 100% of the
-- live (not soft-deleted, id-deduped) Fivetran source rows. A bigger drop
-- means a cleaning rule or the tenant join is eating real data. Returns a row
-- (= failure) per model outside the band.

with src as (
    select 'stg_chatbot_transcript' as model_name, count(distinct id) as src_rows
    from {{ source('chatbot_postgres', 'actions_transcript') }}
    where not coalesce(_fivetran_deleted, false)
    union all
    select 'stg_chatbot_transcript_event', count(distinct id)
    from {{ source('chatbot_postgres', 'actions_transcriptevent') }}
    where not coalesce(_fivetran_deleted, false)
),

mdl as (
    select 'stg_chatbot_transcript' as model_name, count(*) as model_rows
    from {{ ref('stg_chatbot_transcript') }}
    union all
    select 'stg_chatbot_transcript_event', count(*)
    from {{ ref('stg_chatbot_transcript_event') }}
)

select s.model_name, s.src_rows, m.model_rows
from src s
join mdl m using (model_name)
where m.model_rows > s.src_rows
   or m.model_rows < 0.9 * s.src_rows
