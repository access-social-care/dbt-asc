{{
  config(
    materialized='table',
    description='Unsuppressed query segments by LA x segment, all time windows. Staging layer — raw counts for BI. mart_glos_la_query_segments_* slices apply la_suppress on top.'
  )
}}

WITH base AS (
    {{ la_query_segments(months_back=1,  source_model='stg_la_queries_glos', suppress=false) }}
    UNION ALL
    {{ la_query_segments(months_back=3,  source_model='stg_la_queries_glos', suppress=false) }}
    UNION ALL
    {{ la_query_segments(months_back=6,  source_model='stg_la_queries_glos', suppress=false) }}
    UNION ALL
    {{ la_query_segments(months_back=9,  source_model='stg_la_queries_glos', suppress=false) }}
    UNION ALL
    {{ la_query_segments(months_back=12, source_model='stg_la_queries_glos', suppress=false) }}
)

SELECT * FROM base
WHERE SEGMENT NOT IN ('Unmatched', 'Unmapped', 'UNMATCHED', 'Uncategorised')
