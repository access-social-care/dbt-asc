{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Age UK. Mirrors parse_ageuk(): one T1 workbook and one T3 workbook share a date key
  (the middle of the file name); within a pair, T3 rows (Created On, T2 Enquiry Type,
  T3 Enquiry Type) are joined to T1 rows (T1 Enquiry Type, Local Authority) on case number.
  R uses a full join, but a T1-only row has no Created On and is dropped as "missing date",
  so a left join from T3 is equivalent. With a T3 label T1 = T2 Enquiry Type and
  T2 = T3 Enquiry Type; otherwise T1 = T1 Enquiry Type and T2 = T2 Enquiry Type.
  Missing -> '999', lower-cased, joined to the age_uk rows of the universal themes map;
  LA via local_authority_fixer() + cassr_converter().
  Differences from R: R picks ONE pair (latest end date) per run and keeps only months
  absent from history (first-seen). Here every pair is read and, per month, the pair whose file
  ends earliest wins (the earliest delivery is the one R would have read first).
  Known to differ from Snowflake history: the pairs on SharePoint are later re-exports of the same
  months (trailing spaces in T2, more LA filled, a few cases added/removed), so a month's rows are
  not byte-identical to the snapshot Snowflake holds. Not trimmed: R does not trim either.
  Pairing relies on the Fivetran table names (age_uk_t_1_cases_<key>_..., age_uk_t_3_cases_<key>_...).
*/

with t1 as (
    {{ helplines_sources('helplines_data_age_uk_t_1_', {
        'case_number': ['case_number'],
        'their_t1': ['t_1_enquiry_type_t_1_enquiry_type_t_1_enquiry_type_'],
        'la_raw': ['address_1_local_authority_customer_contact_'],
    }, require_columns=['case_number', 'created_on']) }}
),

t3 as (
    {{ helplines_sources('helplines_data_age_uk_t_3_', {
        'case_number': ['case_number_case_case_'],
        'created_on': ['created_on'],
        'their_t2': ['t_2_enquiry_type'],
        'their_t3': ['t_3_enquiry_type'],
    }, require_columns=['case_number_case_case_', 'created_on']) }}
),

t1_keyed as (
    select regexp_replace(source_table, '^helplines_data_age_uk_t_1_cases_?|_?case_advanced_find_view$', '', 'g') as pair_key, *
    from t1
),

t3_keyed as (
    select regexp_replace(source_table, '^helplines_data_age_uk_t_3_cases_?|_?enquiry_type_advanced_find_view$', '', 'g') as pair_key, *
    from t3
),

joined as (
    select
        b.source_table,
        b.pair_key,
        cast(try_cast(left(b.created_on, 19) as timestamp) as date) as date,
        case when b.their_t3 is not null then b.their_t2 else a.their_t1 end as t1_pre,
        case when b.their_t3 is not null then b.their_t3 else b.their_t2 end as t2_pre,
        a.la_raw
    from t3_keyed b
    left join t1_keyed a on a.pair_key = b.pair_key and a.case_number = b.case_number
),

parsed as (
    select
        source_table,
        date,
        lower({{ helplines_sentinel('t1_pre') }}) as t1,
        lower({{ helplines_sentinel('t2_pre') }}) as t2,
        la_raw
    from joined
    where date is not null
),

monthed as (
    select *, {{ helplines_year_month('date') }} as month from parsed
),

picked as (
    select * from {{ helplines_pick_version('monthed', rule='first_seen') }}
)

select
    p.date,
    p.month,
    p.t1,
    p.t2,
    m.ut1,
    m.ut2,
    {{ helplines_cassr_name('l.la_name') }} as la_name,
    l.la_code,
    'age_uk' as org
from picked p
left join {{ helplines_uc_map('age_uk') }} m on m.t1 = p.t1 and m.t2 = p.t2
left join {{ ref('helplines_la_lookup') }} l on l.original_lower = lower(p.la_raw)
