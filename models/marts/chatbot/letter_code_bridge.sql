{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

-- Port of AVA.PUBLIC.LETTER_CODE_BRIDGE (data_uploader.R make_bridge_dim). See macro chatbot_bridge.
{{ chatbot_bridge('lettercode') }}
