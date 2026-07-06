{{
  config(
    materialized='table',
    description='Unsuppressed locality overview by LA x locality, all time windows. Staging layer — raw counts for BI. mart_glos_la_locality_overview_* slices apply la_suppress on top.'
  )
}}

{{ la_locality_overview(months_back=1,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_locality_overview(months_back=3,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_locality_overview(months_back=6,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_locality_overview(months_back=9,  source_model='stg_la_topic_mentions_glos', suppress=false) }}
UNION ALL
{{ la_locality_overview(months_back=12, source_model='stg_la_topic_mentions_glos', suppress=false) }}
