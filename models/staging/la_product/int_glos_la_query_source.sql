{{
  config(
    materialized='table',
    description='Unsuppressed query source by LA x source system, all time windows. Staging layer — raw counts for BI. mart_glos_la_query_source_* slices apply la_suppress on top.'
  )
}}

{{ la_query_source(months_back=1,  source_model='stg_la_queries_glos', suppress=false) }}
UNION ALL
{{ la_query_source(months_back=3,  source_model='stg_la_queries_glos', suppress=false) }}
UNION ALL
{{ la_query_source(months_back=6,  source_model='stg_la_queries_glos', suppress=false) }}
UNION ALL
{{ la_query_source(months_back=9,  source_model='stg_la_queries_glos', suppress=false) }}
UNION ALL
{{ la_query_source(months_back=12, source_model='stg_la_queries_glos', suppress=false) }}
