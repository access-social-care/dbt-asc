{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  data_quality_check.R + data_discriminator.R as one query. Stable periods of the six monthly
  partners (Mencap and RNIB are excluded, ANNUAL_GRAIN_PARTNERS):
    - partner history from MIN_YEAR 2020; a complete partner x month grid from the first to the
      last month present;
    - a month is stable when at least `helplines_min_partners` partners have any data in it;
    - a stable period is a run of at least `helplines_min_consecutive_months` stable months.
  The primary period is the first row: most months, ties broken by the latest end.
  Thresholds: MIN_CONSECUTIVE_MONTHS = 3 and MIN_PARTNERS_REQUIRED = 5, as in data_discriminator.R
  (the charter and README say 6: recorded conflict, code wins). 6 was tried on 2026-10-16 and
  reverted; the owner will revisit once the State of the Nation data is combined (see the tracking issue).
*/

with data as (
    select org, date, month
    from {{ ref('helplines_partner_history') }}
    where org not in ('mencap', 'rnib')
      and date_part('year', date) >= 2020
),

bounds as (
    select date_trunc('month', min(date))::date as lo, date_trunc('month', max(date))::date as hi from data
),

grid_months as (
    select strftime(m, '%Y-%m') as month
    from bounds, generate_series(lo, hi, interval 1 month) as t(m)
),

partners as (
    select distinct org from data
),

coverage as (
    select org, month, count(*) as n_records from data group by 1, 2
),

per_month as (
    select g.month, count(c.org) as n_with_data
    from grid_months g
    cross join partners p
    left join coverage c on c.org = p.org and c.month = g.month
    group by 1
),

flagged as (
    select month, n_with_data, n_with_data >= {{ var('helplines_min_partners', 5) }} as is_stable
    from per_month
),

runs as (
    select *,
           row_number() over (order by month)
         - row_number() over (partition by is_stable order by month) as run_id
    from flagged
)

select
    min(month) as period_start,
    max(month) as period_end,
    count(*) as n_months,
    round(avg(n_with_data), 1) as avg_partners
from runs
where is_stable
group by run_id
having count(*) >= {{ var('helplines_min_consecutive_months', 3) }}
order by n_months desc, period_end desc
