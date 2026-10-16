{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  DuckDB replacement for AVA.PUBLIC.ACCESSAVA: one row per AccessAva
  conversation (transcript_id). Replaces the R chain
  process_raw_data(get_new = TRUE) -> report.Rmd data.csv export ->
  data_uploader.R (prepare_accessava) -> snowflake_write_table.

  Join: int_chatbot_total_convos is the spine and every other int_chatbot_*
  model is LEFT joined on transcript_id. process_raw_data full-joins them and
  then drops rows with no tenant_name; only total_convos carries tenant_name,
  so a row that exists in another feature table but not in total_convos is
  always dropped, which makes the left joins equivalent.

  Steps after the join, in R order:
    1. form demographics (process_raw_data): coalesce(in-chatbot answer, form
       answer) for ethnicity, age_range, person_type, disability_n_y,
       armed_service. Source: raw.chatbot.form_demographics, seeded from
       ascFuncs demographics_matched.RDS (scripts/seed/seed_form_demographics.py).
    2. report.Rmd: drop tenants containing UAT or APDAS_SQUARESPACE; tenant tidy
       (strip _prod/_Prod, underscore to space); entry_month = YYYY-MM of
       created_at.
    3. data_uploader.R prepare_accessava: created_at to Date (UTC day, as the
       data.csv round trip gives); dedupe_by_id (most complete row, ties last).
       The dedupe is a no-op here because every feature model is already one row
       per transcript_id (unique tests); the 2026-08-18 duplicate came from data.csv
       carrying two rows for a re-submitted demographics answer.
  report.Rmd's later gsub of '; ' to ',' and the persona relabel run only on the
  Monday export, after data.csv is written, so they do not reach this table.

  Columns match AVA.PUBLIC.ACCESSAVA minus `...1` (the write.csv row number
  read_csv names `...1`; an artifact of the CSV hop, tie-ordering is not
  reproducible). `county` is all NULL in ACCESSAVA (a stale column of the
  data.csv export that no current function fills) and is kept NULL for column
  parity.

  person_type: the int model mirrors the R lag() ordering bug by default
  (var chatbot_person_type_fix_order); see int_chatbot_person_type.
*/

with spine as (
    select * from {{ ref('int_chatbot_total_convos') }}
    where {{ chatbot_is_reporting_tenant('tenant_name') }}
),

form as (
    select * from {{ source('chatbot_raw', 'form_demographics') }}
)

select
    s.transcript_id,
    {{ chatbot_entry_day('s.created_at') }}                    as created_at,
    {{ chatbot_tenant_report_name('s.tenant_name') }}          as tenant_name,
    cast(s.engaged as double)                                  as engaged,
    s.region,
    cast(null as varchar)                                      as county,
    s.organisation_type,
    comp.completed,
    coalesce(eth.ethnicity, f.ethnicity)                       as ethnicity,
    coalesce(ag.age_range, f.age_range)                        as age_range,
    coalesce(pt.person_type, f.person_type)                    as person_type,
    coalesce(dis.disability_n_y, f.disability_n_y)             as disability_n_y,
    dis.disability_text,
    coalesce(arm.armed_service, f.armed_service)               as armed_service,
    la.la_name,
    ts.dif,
    ch.churn_point,
    ch.churn_category,
    rt.rating,
    fb.user_feedback,
    per.persona,
    scc.supercategories,
    scc.categories,
    scc.csis,
    ld.lettercode,
    ld.letter_downloaded,
    acs.autocomplete_start,
    acf.autocomplete_finish,
    fs.factsheets_templates,
    ra.rasa,
    tep.topic_entry_point,
    cr.consumed_resources,
    aop.age,
    cs.referring_page_url,
    cs.referring_page_parameters,
    {{ chatbot_entry_month(chatbot_entry_day('s.created_at')) }} as entry_month
from spine s
left join {{ ref('int_chatbot_completions') }}          comp on s.transcript_id = comp.transcript_id
left join {{ ref('int_chatbot_ethnicity') }}            eth  on s.transcript_id = eth.transcript_id
left join {{ ref('int_chatbot_age_group') }}            ag   on s.transcript_id = ag.transcript_id
left join {{ ref('int_chatbot_person_type') }}          pt   on s.transcript_id = pt.transcript_id
left join {{ ref('int_chatbot_disability') }}           dis  on s.transcript_id = dis.transcript_id
left join {{ ref('int_chatbot_armed_forces') }}         arm  on s.transcript_id = arm.transcript_id
left join {{ ref('int_chatbot_la') }}                   la   on s.transcript_id = la.transcript_id
left join {{ ref('int_chatbot_time_spent') }}           ts   on s.transcript_id = ts.transcript_id
left join {{ ref('int_chatbot_churn') }}                ch   on s.transcript_id = ch.transcript_id
left join {{ ref('int_chatbot_ratings') }}              rt   on s.transcript_id = rt.transcript_id
left join {{ ref('int_chatbot_feedback') }}             fb   on s.transcript_id = fb.transcript_id
left join {{ ref('int_chatbot_persona') }}              per  on s.transcript_id = per.transcript_id
left join {{ ref('int_chatbot_s_c_csi') }}              scc  on s.transcript_id = scc.transcript_id
left join {{ ref('int_chatbot_letters_downloaded') }}   ld   on s.transcript_id = ld.transcript_id
left join {{ ref('int_chatbot_autocomplete_starts') }}  acs  on s.transcript_id = acs.transcript_id
left join {{ ref('int_chatbot_autocomplete_finish') }}  acf  on s.transcript_id = acf.transcript_id
left join {{ ref('int_chatbot_factsheets_templates') }} fs   on s.transcript_id = fs.transcript_id
left join {{ ref('int_chatbot_rasa') }}                 ra   on s.transcript_id = ra.transcript_id
left join {{ ref('int_chatbot_topic_entry_point') }}    tep  on s.transcript_id = tep.transcript_id
left join {{ ref('int_chatbot_consumed_resources') }}   cr   on s.transcript_id = cr.transcript_id
left join {{ ref('int_chatbot_age_of_person') }}        aop  on s.transcript_id = aop.transcript_id
left join {{ ref('int_chatbot_conversation_source') }}  cs   on s.transcript_id = cs.transcript_id
left join form f                                             on s.transcript_id = f.transcript_id
