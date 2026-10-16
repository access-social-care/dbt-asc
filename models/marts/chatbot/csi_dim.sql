{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.CSI_DIM (make_bridge_dim dim + add_surrogate_key). See macro chatbot_dim.
{{ chatbot_dim('csi_bridge', 'csis', 'csi_id') }}
