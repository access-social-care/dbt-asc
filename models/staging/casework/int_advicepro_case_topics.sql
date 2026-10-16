{{ config(materialized='table') }}

/*
  Distinct case x (group, csi) pairs from the PRE-COLLAPSE report rows, so the
  group<->issue pairing survives (the semicolon collapse destroys it). Port of
  case_topic_raw in case_data/load_advicepro_to_snowflake.R, resolved to the
  canonical taxonomy reference.s_c_csi_map:

    - group format "<prefix> - <category>"; split on the FIRST " - " only
      (category names may contain it). No " - " -> prefix is the whole group,
      category is NULL.
    - supercategory from the prefix via the advicepro_prefix_supercategory seed.
      An unknown prefix gives NULL supercategory_canon: the not_null test on it
      replaces the loader's abort.
    - a csi that is missing or equal to its group is "[Category only]" (a
      legitimate record, not drift).
    - match to s_c_csi_map is case-insensitive and trimmed. A pair not in the
      map keeps NULL s_c_csi_id (taxonomy drift): not dropped, not guessed.
      Duplicate normalised map keys would fan the join out; singular test
      assert_s_c_csi_map_keys_unique replaces the loader's abort.
*/

with pairs as (
    select distinct
        case_reference,
        case_specific_issues_group as csi_group_raw,
        case_specific_issues       as csi_raw
    from {{ ref('stg_advicepro_cases') }}
    where case_specific_issues_group is not null
),

parsed as (
    select
        p.*,
        regexp_replace(csi_group_raw, ' - .*$', '')                          as prefix,
        case when contains(csi_group_raw, ' - ')
             then regexp_replace(csi_group_raw, '^.*? - ', '')
        end                                                                  as category_part,
        case when csi_raw is null or csi_raw = csi_group_raw
             then '[Category only]' else csi_raw end                         as csi_effective
    from pairs p
),

with_supercat as (
    select d.*, s.supercategory as supercat_canon
    from parsed d
    left join {{ ref('advicepro_prefix_supercategory') }} s
        on d.prefix = s.prefix
)

select
    t.case_reference,
    m.s_c_csi_id,
    t.csi_group_raw,
    t.csi_raw,
    t.prefix,
    t.supercat_canon
from with_supercat t
left join {{ source('reference', 's_c_csi_map') }} m
    on  lower(trim(t.supercat_canon)) is not distinct from lower(trim(m.supercategory))
    and lower(trim(t.category_part))  is not distinct from lower(trim(m.category))
    and lower(trim(t.csi_effective))  is not distinct from lower(trim(m.case_specific_issue))
