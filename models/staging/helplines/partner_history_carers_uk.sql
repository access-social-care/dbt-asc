{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Carers UK. Mirrors parse_carersuk(): only LocalAuthority = 'England'; T1 = LevelOneName*,
  T2 = LevelTwoName, missing -> '999', lower-cased, joined to the carers_uk rows of the
  universal themes map. Region-level partner: LA_CODE NULL and LA_NAME NULL (cassr_converter
  finds no CASSR for 'England').
  Full series (reparse/history-from-raw rules, Carers UK October 2026 extracts): Dynamics CRM
  (incidents_v_base*) and MS Forms atomic values fill months that no monthly/annual/Civi report
  covers (months before 2024-10 today), so ongoing months stay exactly as main's reader produced
  them. Rules: Broad_CAT 'Subject(s) Raised' -> T1 = Report_CAT, T2 = 999; 'Issues' -> T1 = 999,
  T2 = Report_CAT; other categories (General, Signposting ...) are not topics and are dropped;
  England or blank Carer Location kept, other nations dropped; the same case x category exported
  twice is one row; day-first dates; keys normalised with reparse norm_t(), exact map key first,
  then the loose key when it maps to a single (UT1, UT2). Not applied: the 2019 CRM code renames
  (1SR_Careplanning -> new_1SR_Careplanning labels) and the Civi atomic table (Nov 2024 on, already
  covered by the monthly Civi reports). Row location is not recorded, so LA_NAME is NULL.
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
),

base as (
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
),

fs_src as (
    {{ helplines_sources('helplines_data_carers_uk_full_series_', {
        'incident_id': ['incident_id'],
        'created_on': ['created_on'],
        'broad_cat': ['broad_cat'],
        'sub_cat': ['sub_cat'],
        'report_cat': ['report_cat'],
        'category': ['category'],
        'carer_location': ['carer_location'],
    }, include_contains=['incidents_v_base', 'msforms']) }}
),

fs_topics as (
    select distinct
        incident_id,
        case when lower(trim(broad_cat)) in ('subject raised', 'subjects raised') then 'subject' else 'issue' end as kind,
        report_cat, category, broad_cat, sub_cat,
        cast(try_strptime(split_part(created_on, ' ', 1), '%d/%m/%Y') as date) as date
    from fs_src
    where lower(trim(broad_cat)) in ('subject raised', 'subjects raised', 'issues')
      and lower(trim(coalesce(carer_location, ''))) in ('', 'england')
),

fs_keys as (
    select
        date, strftime(date, '%Y-%m') as month,
        {{ helplines_norm_t("case when kind = 'subject' then report_cat else null end") }} as k1,
        {{ helplines_norm_t("case when kind = 'issue' then report_cat else null end") }} as k2
    from fs_topics
    where date is not null
),

uc_exact as (
    select k1, k2, ut1, ut2 from (
        select {{ helplines_norm_t('t1') }} as k1, {{ helplines_norm_t('t2') }} as k2, ut1, nullif(ut2, 'NA') as ut2,
               row_number() over (partition by {{ helplines_norm_t('t1') }}, {{ helplines_norm_t('t2') }}) as rn
        from {{ source('reference', 'universal_themes_map') }} where org = 'carers_uk'
    ) where rn = 1
),

uc_loose as (
    select lk1, lk2, any_value(ut1) as ut1, any_value(ut2) as ut2
    from (
        select distinct {{ helplines_norm_t_loose('k1') }} as lk1, {{ helplines_norm_t_loose('k2') }} as lk2, ut1, ut2 from uc_exact
    )
    group by lk1, lk2
    having count(*) = 1
),

fs_mapped as (
    select
        k.date, k.month, k.k1 as t1, k.k2 as t2,
        coalesce(e.ut1, l.ut1) as ut1,
        case when e.k1 is not null then e.ut2 else l.ut2 end as ut2,
        {{ helplines_cassr_name("'England'") }} as la_name,
        cast(null as varchar) as la_code,
        'carers_uk' as org
    from fs_keys k
    left join uc_exact e on e.k1 = k.k1 and e.k2 = k.k2
    left join uc_loose l on e.k1 is null and l.lk1 = {{ helplines_norm_t_loose('k.k1') }} and l.lk2 = {{ helplines_norm_t_loose('k.k2') }}
)

select * from base
union all
select * from fs_mapped where month not in (select distinct month from base where month is not null)
