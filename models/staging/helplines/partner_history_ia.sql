{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Independent Age. Mirrors parse_ia():
    T1 = Main Topic, T2 = Sub Topic (first two columns), joined to the ia rows of the
    universal themes map AS WRITTEN: no sentinel, no lower-casing (the one reader that
    does neither). UT2 is never selected, so UT2 is always NULL.
    Rows with no Opened Date are removed. Date is dd/mm/yyyy.
    unique() over (UT1, T1, T2, county, date, month): rows identical on those fields
    collapse to one. IA case numbers are reused across cases, so this undercounts. It is
    reproduced here because it is what Snowflake holds; the reparse branch removes it
    (see Overlaps, vintages and stable periods in the charter).
    LA: local_authority_fixer() on IA County, then cassr_converter().
  Versions: the files are rolling 12-month windows (1 Apr 24 - 1 Apr 25, 1 May 24 -
  1 May 25 ...) so each month is in up to 12 files; the fullest version of a month is kept.
*/

with src as (
    {{ helplines_sources('helplines_data_independent_age_', {
        'main_topic': ['main_topic_'],
        'sub_topic': ['sub_topic_'],
        'county': ['ia_county'],
        'opened_date': ['opened_date'],
    }) }}
),

parsed as (
    select
        source_table,
        try_strptime(opened_date, '%d/%m/%Y')::date as date,
        main_topic as t1,
        sub_topic as t2,
        county as la_raw
    from src
    where try_strptime(opened_date, '%d/%m/%Y') is not null
),

monthed as (
    select *, {{ helplines_year_month('date') }} as month from parsed
),

-- the version is chosen on rows after unique(), as R counts them
deduped as (
    select distinct source_table, date, month, t1, t2, la_raw from monthed
),

picked as (
    select * from {{ helplines_pick_version('deduped', rule='fullest') }}
)

select
    p.date,
    p.month,
    p.t1,
    p.t2,
    m.ut1,
    cast(null as varchar) as ut2,
    {{ helplines_cassr_name('l.la_name') }} as la_name,
    l.la_code,
    'ia' as org
from picked p
left join {{ helplines_uc_map('ia') }} m on m.t1 = p.t1 and m.t2 = p.t2
left join {{ ref('helplines_la_lookup') }} l on l.original_lower = lower(p.la_raw)
