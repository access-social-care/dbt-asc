{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_s_c_csi (R funcs.R). Supercategories, categories
  and case specific issues from Source = true events joined to Contentful,
  each value corrected through the category_map (int_chatbot_category_map).
  Distinct (supercategory, category, csi) combinations per transcript in
  first-seen (event id) order; the three columns are aligned lists joined
  with '; ' (R: paste(collapse = "; ")). Unmapped values become the text
  'NA', as paste() renders NA in R; a column that is only 'NA' is NULL, as it
  is in ACCESSAVA after the R pipeline's data.csv round trip (read.csv parses
  the text NA as missing). Mixed lists such as 'x; NA' are kept.
*/

with contentful as (
    select distinct id, t_supercategory, t_category, t_casespecificissue
    from {{ source('chatbot_raw', 'contentful_data') }}
    where t_supercategory is not null
),

combos as (
    select
        e.transcript_id,
        c.t_supercategory,
        c.t_category,
        c.t_casespecificissue,
        min(e.id) as first_id
    from {{ ref('stg_chatbot_transcript_event') }} e
    join contentful c on e.contentful_id = c.id
    where e.tags like '%"Source": "true"%'
    group by 1, 2, 3, 4
),

mapped as (
    select
        k.transcript_id,
        k.first_id,
        s.value as supercategory,
        c.value as category,
        i.value as csi
    from combos k
    left join {{ ref('int_chatbot_category_map') }} s
        on s.kind = 's' and s.matcher = k.t_supercategory
    left join {{ ref('int_chatbot_category_map') }} c
        on c.kind = 'c' and c.matcher = k.t_category
    left join {{ ref('int_chatbot_category_map') }} i
        on i.kind = 'csi' and i.matcher = k.t_casespecificissue
)

select
    transcript_id,
    nullif(string_agg(coalesce(supercategory, 'NA'), '; ' order by first_id, supercategory, category, csi), 'NA') as supercategories,
    nullif(string_agg(coalesce(category, 'NA'), '; ' order by first_id, supercategory, category, csi), 'NA') as categories,
    nullif(string_agg(coalesce(csi, 'NA'), '; ' order by first_id, supercategory, category, csi), 'NA') as csis
from mapped
group by 1
