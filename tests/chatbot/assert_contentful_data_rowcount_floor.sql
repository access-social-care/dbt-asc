{{ config(tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Row-count floor: the landed Contentful table held about 14.7k rows (about
-- 12k entries) in October 2026. Fewer than 10000 rows means a truncated land.
-- Returns a row (= failure) when below the floor or when the staging model
-- does not match the raw table.

with counts as (
    select
        (select count(*) from {{ ref('stg_contentful_data') }}) as stg_rows,
        (select count(*) from {{ source('chatbot_contentful', 'contentful_data') }}) as raw_rows
)

select stg_rows, raw_rows
from counts
where stg_rows < 10000
   or stg_rows <> raw_rows
