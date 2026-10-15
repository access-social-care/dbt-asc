{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Ealing Advice Consortium. Mirrors parse_ealing():
    Old export (header "LevelOneName..LevelThreeName, Date of Enquiry, Local Authority"):
      T1 = LevelTwoName, T2 = LevelThreeName, LA from the Local Authority column.
      Fivetran lands that header as source_line 0; it is dropped, and so is source_line 1:
      R reads these with skip = 1 and then removes the first record (rownames id "1").
    New export (Matter Category / Sub Matter Category / ... Case Open Date, no LA column):
      T1 = Matter Category, T2 = Sub Matter Category, LA = 'Ealing'.
  Fivetran maps both layouts onto one column set by position, so old-layout files are
  recognised by that header row. T1/T2 missing -> '999', lower-cased, joined to the
  ealing rows of the universal themes map. LA_NAME is the CASSR name, LA_CODE NULL
  (cassr_converter only; there is no local_authority_fixer for Ealing).
*/

with src as (
    {{ helplines_sources('helplines_data_ealing_', {
        'case_open_date': ['case_open_date'],
        'matter_category': ['matter_category'],
        'sub_matter_category': ['sub_matter_category'],
        'sub_matter_category_group': ['sub_matter_category_group'],
        'local_authority': ['postcode_district'],
    }) }}
),

layout as (
    select source_table, bool_or(source_line = 0 and try_cast(replace(left(case_open_date, 19), 'T', ' ') as timestamp) is null) as old_layout
    from src
    group by 1
),

parsed as (
    select
        s.source_table,
        cast(try_cast(replace(left(s.case_open_date, 19), 'T', ' ') as timestamp) as date) as date,
        {{ helplines_sentinel("case when l.old_layout then s.sub_matter_category_group else s.matter_category end") }} as t1_raw,
        {{ helplines_sentinel('s.sub_matter_category') }} as t2_raw,
        case when l.old_layout then s.local_authority else 'Ealing' end as la_raw
    from src s
    inner join layout l using (source_table)
    where not (l.old_layout and s.source_line <= 1)
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
    lower(p.t1_raw) as t1,
    lower(p.t2_raw) as t2,
    m.ut1,
    m.ut2,
    {{ helplines_cassr_name('p.la_raw') }} as la_name,
    cast(null as varchar) as la_code,
    'ealing' as org
from picked p
left join {{ helplines_uc_map('ealing') }} m on m.t1 = lower(p.t1_raw) and m.t2 = lower(p.t2_raw)
