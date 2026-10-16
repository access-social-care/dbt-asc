{{ config(materialized='table') }}

/*
  DuckDB equivalent of CASEWORK.PUBLIC.ADVICEPRO_CASEWORK: one row per case,
  columns and order as the loader writes them. member_canonical_name is
  referred_from_account mapped through the member_name_mapping seed (an
  unmapped name passes through unchanged). Differences from the Snowflake
  table: no Dynamics backfill (topics / LA for Dynamics-migrated cases) and no
  case_level (not carried by the landed report). See docs/parity/advicepro.md.
*/

select
    c.case_reference,
    c.case_open_month,
    c.case_close_month,
    c.case_status,
    c.case_level,
    c.referred_from_account,
    c.number_of_cases,
    c.la_code,
    c.la_name,
    c.super_category,
    c.case_specific_issues_group,
    c.case_specific_issues,
    c.case_outcome,
    c.opened_on,
    c.closed_on,
    coalesce(m.monday_name, c.referred_from_account) as member_canonical_name
from {{ ref('int_advicepro_cases') }} c
left join {{ ref('member_name_mapping') }} m
    on c.referred_from_account = m.advicepro_name
