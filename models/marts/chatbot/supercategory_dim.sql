{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.SUPERCATEGORY_DIM (make_bridge_dim dim + add_surrogate_key). See macro chatbot_dim.
{{ chatbot_dim('supercategory_bridge', 'supercategories', 'supercategory_id') }}
