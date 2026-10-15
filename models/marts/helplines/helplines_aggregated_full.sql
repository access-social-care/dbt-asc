{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  HELPLINES.PUBLIC.HELPLINES_AGGREGATED_FULL (data_aggregator.R). One row per LA x month x UT1
  with N = records, over the six monthly partners:
    1. months inside the primary stable period (all data if there is none);
    2. year >= 2019 (MIN_YEAR in data_aggregator.R; coverage analysis uses 2020 - recorded conflict);
    3. England only: LA_CODE NULL (region-level partners CAB, Carers UK, Ealing) or starting 'E';
    4. month_date = last day of the month; rows with no month dropped;
    5. UT1 spellings normalised (Advocacy/Appropriate Person -> Advocacy, Assessment ->
       Assessments, Information seeking -> Information Seeking); rows dropped when UT1 is NULL or
       UNMATCHED / unmatched / Unmatched, Cuts, Funding disputes or one of the two retired free-text
       placeholders. Unmapped and Advocacy stay (main branch; the reparse branch also drops Unmapped);
    6. LA_NAME goes through cassr_converter() twice: once on the rows, once on the aggregate
       (after counting, so two names that collapse can leave two rows with the same key, as in R).
*/

with primary_period as (
    select period_start, period_end from {{ ref('helplines_stable_period') }} limit 1
),

in_scope as (
    select p.*
    from {{ ref('helplines_partner_history') }} p
    left join primary_period pp on true
    where p.org not in ('mencap', 'rnib')
      and (pp.period_start is null or (p.month >= pp.period_start and p.month <= pp.period_end))
      and date_part('year', p.date) >= 2019
      and (p.la_code is null or left(p.la_code, 1) = 'E')
      and p.month is not null
),

normalised as (
    select
        last_day(cast(month || '-01' as date)) as month_date,
        case ut1
            when 'Advocacy/Appropriate Person' then 'Advocacy'
            when 'Assessment' then 'Assessments'
            when 'Information seeking' then 'Information Seeking'
            else ut1
        end as ut1,
        {{ helplines_cassr_name('la_name') }} as la_name
    from in_scope
),

kept as (
    select * from normalised
    where ut1 is not null
      and ut1 not in (
          'UNMATCHED', 'unmatched', 'Unmatched', 'Cuts', 'Funding disputes',
          'we need to remove this as it is unclear how we can map it- it merges 2 key areas',
          'do not include as not collecting benefits data'
      )
),

counted as (
    select la_name, month_date, ut1, count(*) as n
    from kept
    group by 1, 2, 3
)

select
    month_date,
    ut1,
    cast(n as double) as n,
    {{ helplines_cassr_name('la_name') }} as la_name
from counted
