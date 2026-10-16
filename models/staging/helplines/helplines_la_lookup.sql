{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  ascFuncs::local_authority_fixer() as a lookup table: lower-cased raw LA text ->
  (la_code, la_name). Three stages, first hit wins for a key, mirroring the R code:
    1. exact (case-insensitive) match to an ONS LAD name        (level 'LA')
    2. match to any ONS geography name (wards, places)          (level 'not LA')
    3. la_name_map alias, no digits, then resolved via stage 2   (level 'la_map')
  A key can map to several codes in stages 2/3 (a ward name used in many LAs); R
  then fans the partner row out once per code, so this table does too.
  The string-distance stage only produces a to-verify list in R, not matches.
  cassr_converter() is applied by the partner models (reference.main.la_to_cassr).
*/

with stage1 as (
    select distinct lower(lad_name) as original_lower, lad_code as la_code, lad_name as la_name, 'LA' as level
    from {{ source('reference_la', 'ons_lad_2025') }}
),
stage2 as (
    select distinct g.name as original_lower, g.la_code, g.geo_name as la_name, 'not LA' as level
    from {{ source('reference_la', 'geo_to_la') }} g
    where g.name not in (select original_lower from stage1)
),
stage3 as (
    select distinct m.original as original_lower, g.la_code, g.geo_name as la_name, 'la_map' as level
    from {{ source('reference_la', 'la_name_map') }} m
    inner join {{ source('reference_la', 'geo_to_la') }} g on g.name = m.final
    where m.final <> '999'
      and not regexp_matches(m.original, '[0-9]')
      and m.original not in (select original_lower from stage1)
      and m.original not in (select original_lower from stage2)
)
select * from stage1
union all select * from stage2
union all select * from stage3
