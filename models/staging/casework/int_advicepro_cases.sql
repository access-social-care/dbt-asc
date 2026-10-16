{{ config(materialized='table') }}

/*
  One row per case_reference. Port of the "Normalise to Case Level" half of
  case_data/load_advicepro_to_snowflake.R (without the Dynamics backfill, which
  reads CASEWORK_STAGING data that is not on MotherDuck - see
  docs/parity/advicepro.md).

  - LA: standardised (int_advicepro_la_lookup, falling back to the raw value as
    the loader's coalesce() does), then converted to the CASSR name through
    reference.la_to_cassr (lower-cased join). A name with no CASSR gets NULL
    la_name; la_code is kept from the standardiser.
  - Topics, categories and outcomes are collapsed over ALL of a case's rows
    (every annual report) to sorted, de-duplicated ';'-delimited strings.
    Sort is binary (C-locale) order.
  - Every other column comes from ONE row per case: closed beats Active, then
    latest close month (open cases sort as '0000/00'), then latest open month.
    Remaining ties: earliest report_year, then local authority text.
  - opened_on = first day of the open month; closed_on = last day of the close
    month, NULL for open cases. Source months are 'YYYY/MM'.
  - number_of_cases is 1 on every row. case_level is not in the landed report
    (landing_log: columns_dropped), so it is NULL here.
*/

with base as (
    select
        s.*,
        coalesce(lk.la_name, s.local_authority) as la_name_std,
        lk.la_code
    from {{ ref('stg_advicepro_cases') }} s
    left join {{ ref('int_advicepro_la_lookup') }} lk
        on s.local_authority = lk.local_authority
),

with_cassr as (
    select
        r.*,
        c.cassr_name as la_name
    from base r
    left join {{ source('reference_geo', 'la_to_cassr') }} c
        on lower(r.la_name_std) = c.la_name
),

topics as (
    select
        case_reference,
        nullif(string_agg(distinct super_category, ';' order by super_category), '')                         as super_category,
        nullif(string_agg(distinct case_specific_issues_group, ';' order by case_specific_issues_group), '') as case_specific_issues_group,
        nullif(string_agg(distinct case_specific_issues, ';' order by case_specific_issues), '')             as case_specific_issues,
        nullif(string_agg(distinct case_outcome, ';' order by case_outcome), '')                             as case_outcome
    from with_cassr
    group by case_reference
),

best as (
    select *
    from with_cassr
    qualify row_number() over (
        partition by case_reference
        order by
            case when case_status = 'Active' then 2
                 when case_status is null then null
                 else 1 end nulls last,
            coalesce(case_close_month, '0000/00') desc,
            case_open_month desc nulls last,
            report_year,
            local_authority nulls last
    ) = 1
),

dated as (
    select
        b.*,
        try_cast(replace(b.case_open_month, '/', '-') || '-01' as date) as opened_on
    from best b
)

select
    d.case_reference,
    d.case_open_month,
    d.case_close_month,
    d.case_status,
    cast(null as varchar)                                       as case_level,
    d.referred_from_account,
    cast(1 as integer)                                          as number_of_cases,
    d.la_code,
    d.la_name,
    t.super_category,
    t.case_specific_issues_group,
    t.case_specific_issues,
    t.case_outcome,
    d.opened_on,
    case when d.case_close_month is null then null
         else cast(date_trunc('month',
                  try_cast(replace(d.case_close_month, '/', '-') || '-01' as date))
                  + interval 1 month - interval 1 day as date)
    end                                                         as closed_on
from dated d
join topics t using (case_reference)
