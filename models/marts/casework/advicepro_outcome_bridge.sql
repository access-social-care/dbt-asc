{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.OUTCOME_BRIDGE #}
{{ casework_bridge(ref('advicepro_casework'), 'case_outcome') }}
