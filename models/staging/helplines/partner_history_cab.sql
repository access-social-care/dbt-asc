{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Citizens Advice. Mirrors parse_cab():
    AICPart1 = 'Health & community care' and one of nine English Government Regions
    (mapped to ONS spelling; "All Regions", Wales, PCCs, Channel Islands drop out);
    T1 = AICPart2, T2 = AICPart3, lower-cased; one output row per client (uncount).
    The region is the location: LA_NAME = region, LA_CODE NULL, no cassr conversion.
  Versions: the two Tableau exports overlap from 2023-04; the fuller month wins.
*/

with src as (
    {{ helplines_sources('helplines_data_citizens_advice_', {
        'month_of_date': ['month_of_date'],
        'aicpart_1': ['aicpart_1'],
        'aicpart_2': ['aicpart_2'],
        'aicpart_3': ['aicpart_3'],
        'government_region': ['government_region'],
        'clients': ['clients'],
    }) }}
),

region_map as (
    select * from (values
        ('North East', 'North East'), ('North West', 'North West'),
        ('Yorkshire & the Humber', 'Yorkshire and The Humber'),
        ('East Midlands', 'East Midlands'), ('West Midlands', 'West Midlands'),
        ('Eastern', 'East of England'), ('London', 'London'),
        ('South East', 'South East'), ('South West', 'South West')
    ) as t(raw_region, ons_region)
),

filtered as (
    select
        s.source_table,
        try_strptime('01 ' || s.month_of_date, '%d %B %Y')::date as date,
        {{ helplines_sentinel('s.aicpart_2') }} as t1_raw,
        {{ helplines_sentinel('s.aicpart_3') }} as t2_raw,
        r.ons_region as la_name,
        cast(s.clients as integer) as clients
    from src s
    inner join region_map r on r.raw_region = s.government_region
    where s.aicpart_1 = 'Health & community care'
),

monthed as (
    select *, {{ helplines_year_month('date') }} as month from filtered
),

picked as (
    select * from {{ helplines_pick_version('monthed', rule='fullest', weight='clients') }}
),

expanded as (
    select date, month, lower(t1_raw) as t1, lower(t2_raw) as t2, la_name,
           unnest(generate_series(1, clients)) as _n
    from picked
    where clients > 0
)

select
    e.date,
    e.month,
    e.t1,
    e.t2,
    m.ut1,
    m.ut2,
    e.la_name,
    cast(null as varchar) as la_code,
    'cab' as org
from expanded e
left join {{ helplines_uc_map('cab') }} m on m.t1 = e.t1 and m.t2 = e.t2
