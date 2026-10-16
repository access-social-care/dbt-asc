{{ config(enabled = (target.type == 'duckdb')) }}
{#
  Replaces the loader's abort on duplicate case-insensitive S_C_CSI_MAP keys:
  a duplicate would fan the case topic join out. Clean the map, do not work
  around it here.
#}
select
    lower(trim(supercategory))        as supercategory_key,
    lower(trim(category))             as category_key,
    lower(trim(case_specific_issue))  as case_specific_issue_key,
    count(*)                          as n
from {{ source('reference', 's_c_csi_map') }}
group by 1, 2, 3
having count(*) > 1
