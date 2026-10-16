{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_process_tr(get_new = TRUE). One row per transcript
  (widget session), cleaned.

  Replaces: the id-paged download loop, secondary_tables/transcript-*.RDS,
  last_day_transcript.txt and actions_transcript.fst. dbt reads the Fivetran
  copy of Postgres directly, so there is nothing to page or cache.

  Rules, in R order:
    1. dedupe by id (R: !duplicated(id)); Fivetran soft deletes dropped.
    2. test-user removal: names miriam/fin/test/kari/testing taken from
       raw_tracker_record user_input (macro chatbot_tracker_name_answer).
    3. entry_day / entry_month / entry_week (UTC day, macros).
    4. tenant_name = tenants_tenant.organisation_name via tenant_id; drop NULL
       tenant and 'AccessAva #test'; tenant_id dropped.
  Not applied here: the report.Rmd step that builds data.csv / ACCESSAVA drops
  tenants containing UAT or APDAS_SQUARESPACE and tidies names. It is exposed
  as is_reporting_tenant (filter on it for ACCESSAVA parity); organisation_name
  is already clean, so the _prod stripping (macro chatbot_tenant_report_name)
  is a no-op today.

  Grain: one row per id. `transcript_id` is an alias of `id` (the feature
  functions select(transcript_id = id)).
  is_engaged = the tr_total_convos rule (extract_tr_data.R: raw_tracker_record
  != "{}") with its INTENT preserved: Postgres via RPostgres gave an empty
  tracker as '{}', the Fivetran copy gives '[]' (and non-empty trackers are a
  JSON array, not a Postgres text-array literal). A literal <> '{}' would be
  TRUE for all 4.7M rows, so empty is '{}' or '[]'.
*/

with src as (
    select *
    from {{ source('chatbot_postgres', 'actions_transcript') }}
    qualify row_number() over (
        partition by id order by _fivetran_synced desc nulls last, updated_at desc nulls last) = 1
),

live as (
    select * from src where not coalesce(_fivetran_deleted, false)
),

test_ids as (
    select id
    from live
    where {{ chatbot_tracker_name_answer('raw_tracker_record') }} in {{ chatbot_test_users() }}
),

tenants as (
    select id as tenant_id, organisation_name as tenant_name
    from {{ source('chatbot_postgres', 'tenants_tenant') }}
    where not coalesce(_fivetran_deleted, false)
),

dated as (
    select
        l.*,
        {{ chatbot_entry_day('l.created_at') }} as entry_day
    from live l
    where l.id not in (select id from test_ids)
)

select
    d.id,
    d.id as transcript_id,
    d.created_at,
    d.updated_at,
    d.user_id,
    d.rasa_tracker_sender_id,
    d.referring_page,
    d.user_feedback,
    d.user_experience,
    d.ethnicity,
    d.age_range,
    d.person_type,
    d.disability,
    d.armed_service,
    d.local_authority_name,
    d.conversation_record,
    d.raw_tracker_record,
    (d.raw_tracker_record not in ('{}', '[]')) as is_engaged,
    d.entry_day,
    {{ chatbot_entry_month('d.entry_day') }} as entry_month,
    {{ chatbot_entry_week('d.entry_day') }} as entry_week,
    t.tenant_name,
    {{ chatbot_is_reporting_tenant('t.tenant_name') }} as is_reporting_tenant
from dated d
left join tenants t on d.tenant_id = t.tenant_id
where t.tenant_name is not null
  and t.tenant_name <> 'AccessAva #test'
