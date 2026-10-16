{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  RNIB (annual-grain partner; never part of the monthly mart). Same two inputs as Mencap:
    1. Seeded history (org = 'rnib'): UT1/UT2 from the seed, '999' sentinel, no lower-casing,
       LA_NAME = cassr_converter(la), LA_CODE = la_code.
    2. Annual "Data sheet" submissions, parse_rnib(): T1 = Enquiry Issue, T2 always '999'
       (RNIB has no sub-category), rows with no Enquiry Date removed, lower-cased, joined to the
       rnib rows of the universal themes map; LA via local_authority_fixer() + cassr_converter().
  Only tables that have the Data sheet columns are read; the pivot and Table sheets of the
  same workbooks are skipped. Fuller month wins, ties keep the history (reconcile_annual_months).
*/

with seed as (
    {{ helplines_sources('helplines_data_mencap_rnib_', {
        'date': ['date'],
        'org': ['org'],
        't1': ['t_1'],
        't2': ['t_2'],
        'ut1': ['ut_1'],
        'ut2': ['ut_2'],
        'la': ['la'],
        'la_code': ['la_code'],
    }, include_contains=['history']) }}
),

annual as (
    {{ helplines_sources('helplines_data_rnib_', {
        'enquiry_date': ['enquiry_date'],
        'enquiry_issue': ['enquiry_issue'],
        'local_authority': ['local_authority'],
    }, require_columns=['enquiry_issue', 'enquiry_date']) }}
),

seed_rows as (
    select
        '000_history_seed' as source_table,
        cast(try_cast(left(date, 19) as timestamp) as date) as date,
        {{ helplines_sentinel('t1') }} as t1,
        {{ helplines_sentinel('t2') }} as t2,
        ut1,
        ut2,
        {{ helplines_cassr_name('la') }} as la_name,
        la_code
    from seed
    where org = 'rnib'
),

annual_rows as (
    select
        a.source_table,
        cast(try_cast(left(a.enquiry_date, 19) as timestamp) as date) as date,
        lower({{ helplines_sentinel('a.enquiry_issue') }}) as t1,
        '999' as t2,
        a.local_authority as la_raw
    from annual a
    where try_cast(left(a.enquiry_date, 19) as timestamp) is not null
),

annual_mapped as (
    select
        r.source_table,
        r.date,
        r.t1,
        r.t2,
        m.ut1,
        m.ut2,
        {{ helplines_cassr_name('l.la_name') }} as la_name,
        l.la_code
    from annual_rows r
    left join {{ helplines_uc_map('rnib') }} m on m.t1 = r.t1 and m.t2 = r.t2
    left join {{ ref('helplines_la_lookup') }} l on l.original_lower = lower(r.la_raw)
),

unioned as (
    select *, {{ helplines_year_month('date') }} as month from (
        select * from seed_rows
        union all
        select * from annual_mapped
    )
),

picked as (
    select * from {{ helplines_pick_version('unioned', rule='fullest') }}
)

select date, month, t1, t2, ut1, ut2, la_name, la_code, 'rnib' as org
from picked
