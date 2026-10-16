{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Carers UK. Mirrors parse_carersuk(): only LocalAuthority = 'England'; T1 = LevelOneName*,
  T2 = LevelTwoName, missing -> '999', lower-cased, joined to the carers_uk rows of the
  universal themes map. Region-level partner: LA_CODE NULL and LA_NAME NULL (cassr_converter
  finds no CASSR for 'England').
  Sources: the monthly xlsx reports, the Civi monthly reports and the cumulative "annual
  report to <month>" workbooks. The "full_series" tables (CRM incidents, MS Forms, Civi atomic
  values) belong to the reparse branch (reparse/history-from-raw), not to main's reader, and
  are excluded until that branch merges. Every cumulative report repeats earlier months:
  the fullest version of a month is kept.
*/

with src as (
    {{ helplines_sources('helplines_data_carers_uk_', {
        'date_of_enquiry': ['date_of_enquiry'],
        'level_one_name': ['level_one_name_'],
        'level_two_name': ['level_two_name'],
        'local_authority': ['local_authority'],
    }, exclude_contains=['full_series']) }}
),

parsed as (
    select
        source_table,
        cast(try_cast(left(date_of_enquiry, 19) as timestamp) as date) as date,
        lower({{ helplines_sentinel('level_one_name') }}) as t1,
        lower({{ helplines_sentinel('level_two_name') }}) as t2
    from src
    where local_authority = 'England'
),

monthed as (
    select *, {{ helplines_year_month('date') }} as month from parsed
),

picked as (
    select * from {{ helplines_pick_version('monthed', rule='fullest') }}
)

select
    p.date,
    p.month,
    p.t1,
    p.t2,
    m.ut1,
    m.ut2,
    {{ helplines_cassr_name("'England'") }} as la_name,
    cast(null as varchar) as la_code,
    'carers_uk' as org
from picked p
left join {{ helplines_uc_map('carers_uk') }} m on m.t1 = p.t1 and m.t2 = p.t2
