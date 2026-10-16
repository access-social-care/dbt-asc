{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Mencap (annual-grain partner; never part of the monthly mart). Two inputs, as in R:
    1. The seeded history (compiled full dataset, one_time/seed_mencap_rnib_history.R):
       UT1/UT2 are taken from the seed, NOT re-joined (the same T1 text mapped to
       different UT1s year to year); T1/T2 get the '999' sentinel but are not lower-cased;
       LA_NAME = cassr_converter(la), LA_CODE = la_code.
    2. Annual submissions, parse_mencap(): LevelOneCodeName* in (Community Care,
       CORONAVIRUS, Safeguarding, Social Care) (Health stays out); T1 = LevelTwoCodeName,
       T2 = LevelThreeCodeName; rows with no Enquiry Date removed; lower-cased; joined to the
       mencap rows of the universal themes map; LA via local_authority_fixer() +
       cassr_converter() on Funding Authority.
  mencap_reader() / reconcile_annual_months(): for a month present in both the history and a
  submission, the version with MORE rows wins; ties keep the history. Same rule here
  (fullest month; the seed is labelled so that it sorts first on a tie).
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
    {{ helplines_sources('helplines_data_mencap_', {
        'enquiry_date': ['enquiry_date'],
        'level_one': ['level_one_code_name_'],
        'level_two': ['level_two_code_name'],
        'level_three': ['level_three_code_name'],
        'funding_authority': ['funding_authority'],
    }, exclude_contains=['history'], require_columns=['level_one_code_name_']) }}
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
    where org = 'mencap'
),

annual_rows as (
    select
        a.source_table,
        cast(try_cast(left(a.enquiry_date, 19) as timestamp) as date) as date,
        lower({{ helplines_sentinel('a.level_two') }}) as t1,
        lower({{ helplines_sentinel('a.level_three') }}) as t2,
        a.funding_authority as la_raw
    from annual a
    where a.level_one in ('Community Care', 'CORONAVIRUS', 'Safeguarding', 'Social Care')
      and try_cast(left(a.enquiry_date, 19) as timestamp) is not null
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
    left join {{ helplines_uc_map('mencap') }} m on m.t1 = r.t1 and m.t2 = r.t2
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

select date, month, t1, t2, ut1, ut2, la_name, la_code, 'mencap' as org
from picked
