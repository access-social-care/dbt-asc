{{ config(tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Format-drift guard. The Fivetran copy stores an empty raw_tracker_record as
-- '[]' (R/RPostgres saw '{}'); if the connector changes that marker again,
-- is_engaged flips to TRUE for nearly every row. Real engaged sessions are
-- well under 1% of transcripts, so fail if more than 5% are flagged.

select engaged_rows, total_rows
from (
    select
        count(*) filter (where is_engaged) as engaged_rows,
        count(*) as total_rows
    from {{ ref('stg_chatbot_transcript') }}
)
where engaged_rows > 0.05 * total_rows
