{{ config(materialized='table') }}

/*
  AdvicePro report rows (one per case x topic, all annual reports) with the
  loader's cleaning rules applied. Port of the first mutate() in
  case_data/load_advicepro_to_snowflake.R:
    - '9999/12' close-month sentinel -> NULL
    - '' and '[Not Specified]' -> NULL in every character column
  Still one row per case x topic: topics are collapsed in int_advicepro_cases
  and kept paired in int_advicepro_case_topics.
*/

select
    {{ advicepro_clean('case_reference') }}                                 as case_reference,
    {{ advicepro_clean('case_status') }}                                    as case_status,
    {{ advicepro_clean('case_open_month') }}                                as case_open_month,
    {{ advicepro_clean("nullif(case_close_month, '9999/12')") }}            as case_close_month,
    {{ advicepro_clean('local_authority') }}                                as local_authority,
    {{ advicepro_clean('super_category') }}                                 as super_category,
    {{ advicepro_clean('case_specific_issues_group') }}                     as case_specific_issues_group,
    {{ advicepro_clean('case_specific_issues') }}                           as case_specific_issues,
    {{ advicepro_clean('case_outcome') }}                                   as case_outcome,
    {{ advicepro_clean('referred_from_account') }}                          as referred_from_account,
    report_year
from {{ source('advicepro_raw', 'cases_raw') }}
