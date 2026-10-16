{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.TOPIC_ENTRY_POINT_DIM (make_bridge_dim dim + add_surrogate_key). See macro chatbot_dim.
{{ chatbot_dim('topic_entry_point_bridge', 'topic_entry_point', 'topic_entry_point_id') }}
