{{
    config(
        materialized = 'table'
    )
}}

select
    case_reference,
    case_open_month,
    case_close_month,
    case_status,
    case_level,
    case_outcome,
    number_of_cases,
    referred_from_account,
    member_canonical_name,
    local_authority,
    la_code,
    la_name_original,
    super_category,
    case_specific_issues_group,
    case_specific_issues
from {{ source('casework', 'advicepro_casework') }}
