{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Scope. Mirrors parse_scope(): rows whose LevelTwoCodeName is 'Social care - Wales' (and rows
  with no LevelTwoCodeName, which dplyr::filter drops as NA) are removed; T1 =
  lower(LevelTwoCodeName), T2 always '999'; joined to the scope rows of the universal themes
  map; LA via local_authority_fixer() then cassr_converter().
  Differences from R: R reads only the LAST sheet of each workbook (a known limit, see the
  charter); Fivetran gives one table per sheet and the sheet order is lost, so every
  data sheet (scope_<date range> tables; data_sheet_dict / data_sheet_example are excluded)
  is read and the fullest version of each month is kept.
*/

with src as (
    {{ helplines_sources('helplines_data_scope_', {
        'date': ['date_', 'date'],
        'level_two_code_name': ['level_two_code_name'],
        'local_authority': ['local_authority_', 'local_authority'],
    }, exclude_contains=['data_sheet_dict', 'data_sheet_example']) }}
),

parsed as (
    select
        source_table,
        cast(try_cast(left(date, 19) as timestamp) as date) as date,
        lower(level_two_code_name) as t1,
        local_authority as la_raw
    from src
    where level_two_code_name <> 'Social care - Wales'
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
    '999' as t2,
    m.ut1,
    m.ut2,
    {{ helplines_cassr_name('l.la_name') }} as la_name,
    l.la_code,
    'scope' as org
from picked p
left join {{ helplines_uc_map('scope') }} m on m.t1 = p.t1 and m.t2 = '999'
left join {{ ref('helplines_la_lookup') }} l on l.original_lower = lower(p.la_raw)
