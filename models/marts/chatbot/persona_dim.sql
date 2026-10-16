{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.PERSONA_DIM (make_bridge_dim dim + add_surrogate_key). See macro chatbot_dim.
{{ chatbot_dim('persona_bridge', 'persona', 'persona_id') }}
