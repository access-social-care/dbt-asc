{{
  config(
    materialized='table',
    description='Unsuppressed queries over time by LA x calendar month, all time windows. Staging layer — raw counts for BI. mart_glos_la_queries_over_time_* slices apply la_suppress on top.'
  )
}}

{{ la_queries_over_time(months_back=1,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_queries_over_time(months_back=3,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_queries_over_time(months_back=6,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_queries_over_time(months_back=9,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_queries_over_time(months_back=12, source_model='stg_la_topic_mentions_glos', suppress=false) }}
