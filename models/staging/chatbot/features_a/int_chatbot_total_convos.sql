{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_total_convos(). One row per conversation
  (transcript_id): every engaged transcript (non-empty tracker, the R
  tr_total_convos.fst snapshot, now derived live) plus the first event of every
  transcript in stg_chatbot_transcript_event; where both exist the transcript
  row wins, as bind_rows + slice(1) does in R. Then left-joined to tenant
  region / organisation_type (R: tenants_tenant.RDS built by
  chatbot_get_secondary_tables, which pivots tenants_membership -> subgroup ->
  group into one column per group type, subgroups comma-joined).

  Columns: transcript_id, created_at, tenant_name, engaged (always 1), region,
  organisation_type. Not filtered to reporting tenants (join on
  is_reporting_tenant via tenant_name downstream; use stg_chatbot_transcript).
*/

with tr_part as (
    select transcript_id, created_at, tenant_name
    from {{ ref('stg_chatbot_transcript') }}
    where is_engaged
),

ev_part as (
    select transcript_id, created_at, tenant_name
    from {{ ref('stg_chatbot_transcript_event') }}
    qualify row_number() over (partition by transcript_id order by created_at, id) = 1
),

unioned as (
    select 1 as src, * from tr_part
    union all
    select 2 as src, * from ev_part
),

first_row as (
    select transcript_id, created_at, tenant_name
    from unioned
    qualify row_number() over (partition by transcript_id order by src, created_at) = 1
),

tenants as (
    select id as tenant_id, organisation_name as tenant_name
    from {{ source('chatbot_postgres', 'tenants_tenant') }}
    where not coalesce(_fivetran_deleted, false)
),

group_values as (
    select
        t.tenant_name,
        lower(replace(g.name, ' ', '_')) as group_type,
        s.name as subgroup_name
    from {{ source('chatbot_postgres', 'tenants_membership') }} m
    join tenants t on m.tenant_id = t.tenant_id
    left join {{ source('chatbot_postgres', 'tenants_subgroup') }} s
        on m.subgroup_id = s.id and not coalesce(s._fivetran_deleted, false)
    left join {{ source('chatbot_postgres', 'tenants_group') }} g
        on s.group_id = g.id and not coalesce(g._fivetran_deleted, false)
    where not coalesce(m._fivetran_deleted, false)
),

tenant_groups as (
    select
        tenant_name,
        string_agg(subgroup_name, ',' order by subgroup_name) filter (where group_type = 'region') as region,
        string_agg(subgroup_name, ',' order by subgroup_name) filter (where group_type = 'organisation_type') as organisation_type
    from group_values
    group by tenant_name
)

select
    f.transcript_id,
    f.created_at,
    f.tenant_name,
    1 as engaged,
    tg.region,
    tg.organisation_type
from first_row f
left join tenant_groups tg on f.tenant_name = tg.tenant_name
