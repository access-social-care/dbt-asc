{{ config(materialized='table') }}

/*
  One row per distinct non-NULL AdvicePro local_authority value with the LA
  name/code the loader would give it. Port of ascFuncs::local_authority_fixer()
  (good_list only; its string-distance "to_verify" proposals are never applied):

    1. exact case-insensitive match to an ONS LAD name        (la_25)
    2. else exact match to any ONS geography name             (all_geo_to_la_map)
    3. else, for values with no digit, via the curated alias map (la_map; the
       '999' alias means "not an LA" and is dropped), then to ONS geography
  Unmatched values have NULL la_name/la_code here; int_advicepro_cases keeps
  the raw value, as the loader's coalesce() does.
  reference.la_name_map has one duplicate alias; the loader's unique() keeps
  one row per (original, name, code), as the DISTINCT below does.
*/

with las as (
    select distinct local_authority
    from {{ ref('stg_advicepro_cases') }}
    where local_authority is not null
),

s1 as (
    select l.local_authority, o.lad_code as la_code, o.lad_name as la_name
    from las l
    join {{ source('reference_geo', 'ons_lad_2025') }} o
        on lower(l.local_authority) = lower(o.lad_name)
),

s2 as (
    select l.local_authority, g.la_code, g.geo_name as la_name
    from las l
    join {{ source('reference_geo', 'geo_to_la') }} g
        on lower(l.local_authority) = g.name
    where l.local_authority not in (select local_authority from s1)
),

s3 as (
    select distinct l.local_authority, g.la_code, g.geo_name as la_name
    from las l
    join {{ source('reference_geo', 'la_name_map') }} m
        on lower(l.local_authority) = m.original
    join {{ source('reference_geo', 'geo_to_la') }} g
        on m.final = g.name
    where m.final <> '999'
      and not regexp_matches(l.local_authority, '[0-9]')
      and l.local_authority not in (select local_authority from s1)
      and l.local_authority not in (select local_authority from s2)
),

matched as (
    select * from s1
    union all
    select * from s2
    union all
    select * from s3
)

select
    l.local_authority,
    m.la_name,
    m.la_code
from las l
left join (select distinct * from matched) m
    on l.local_authority = m.local_authority
