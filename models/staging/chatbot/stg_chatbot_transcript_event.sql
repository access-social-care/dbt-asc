{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_process_tr_ev(get_new = TRUE). One row per
  transcript event (bot or user message), cleaned. This is the table every
  chatbot_* feature function consumes as `tr_ev`.

  Replaces: the id-paged download loop, secondary_tables/transcript_event-*.RDS,
  last_id_transcript_event.txt, the per-transcript .txt dump in
  chatbot_get_transcript_event, process_in_chunks and
  actions_transcript_events.fst.

  Rules, in R order:
    1. dedupe by id (R: unique()); Fivetran soft deletes dropped.
    2. test-user removal: a transcript is removed when the event immediately
       after a "What should I call you?" message (by id within transcript) is
       one of the test names. Removes the whole transcript's events. This is
       independent of the tr rule in stg_chatbot_transcript, as in R.
    3. entry_day / entry_month / entry_week (UTC day, macros).
    4. contentful_id = raw_record .cms_response.id (macro chatbot_json_string),
       lettercode joined from Contentful (id -> lettercode, non-null, distinct).
       cms_component_type is dropped, as in R.
    5. tenant_name = tenants_tenant.organisation_name via the EVENT's own
       tenant_id (not the transcript's); drop NULL tenant and
       'AccessAva #test'; tenant_id dropped.

  Deliberate difference from R: contentful_id is NULL where raw_record has no
  .cms_response.id; R's jq call produced the text "null". lettercode is
  identical either way (neither joins).
*/

with live as (
    -- Soft deletes dropped before the latest-row pick (see stg_chatbot_transcript).
    select *
    from {{ source('chatbot_postgres', 'actions_transcriptevent') }}
    where not coalesce(_fivetran_deleted, false)
    qualify row_number() over (
        partition by id order by _fivetran_synced desc nulls last) = 1
),

lagged as (
    select
        transcript_id,
        lower(text) as text_lower,
        lag(text, 1) over (partition by transcript_id order by id) as prev_text
    from live
),

test_ids as (
    select distinct transcript_id
    from lagged
    where coalesce(regexp_matches(prev_text, 'What should I call you?'), false)
      and text_lower in {{ chatbot_test_users() }}
),

lettercodes as (
    select distinct lettercode, id as contentful_id
    from {{ source('chatbot_raw', 'contentful_data') }}
    where lettercode is not null and id is not null
),

tenants as (
    select id as tenant_id, organisation_name as tenant_name
    from {{ source('chatbot_postgres', 'tenants_tenant') }}
    where not coalesce(_fivetran_deleted, false)
),

enriched as (
    select
        l.*,
        {{ chatbot_entry_day('l.created_at') }} as entry_day,
        {{ chatbot_json_string('l.raw_record', '$.cms_response.id') }} as contentful_id
    from live l
    where l.transcript_id not in (select transcript_id from test_ids)
)

select
    e.id,
    e.transcript_id,
    e.created_at,
    e.bot_or_user,
    e.text,
    e.content_type,
    e.raw_record,
    e.intent_name,
    e.confidence,
    e.tags,
    e.entry_day,
    {{ chatbot_entry_month('e.entry_day') }} as entry_month,
    {{ chatbot_entry_week('e.entry_day') }} as entry_week,
    e.contentful_id,
    lc.lettercode,
    t.tenant_name,
    {{ chatbot_is_reporting_tenant('t.tenant_name') }} as is_reporting_tenant
from enriched e
left join lettercodes lc on e.contentful_id = lc.contentful_id
left join tenants t on e.tenant_id = t.tenant_id
where t.tenant_name is not null
  and t.tenant_name <> 'AccessAva #test'
