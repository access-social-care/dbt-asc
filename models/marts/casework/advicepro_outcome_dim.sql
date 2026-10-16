{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.OUTCOME_DIM #}
{{ casework_dim('advicepro_outcome_bridge', 'case_outcome', 'outcome_id') }}
