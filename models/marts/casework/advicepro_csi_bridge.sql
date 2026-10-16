{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.CSI_BRIDGE (not the chatbot csi_bridge in the same schema) #}
{{ casework_bridge(ref('advicepro_casework'), 'case_specific_issues') }}
