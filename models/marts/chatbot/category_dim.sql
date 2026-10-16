{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.CATEGORY_DIM (make_bridge_dim dim + add_surrogate_key). See macro chatbot_dim.
{{ chatbot_dim('category_bridge', 'categories', 'category_id') }}
