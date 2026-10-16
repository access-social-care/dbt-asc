{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.CONSUMED_RESOURCE_DIM (make_bridge_dim dim + add_surrogate_key). See macro chatbot_dim.
{{ chatbot_dim('consumed_resource_bridge', 'consumed_resources', 'consumed_resource_id') }}
