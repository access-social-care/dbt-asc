{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.CSI_GROUP_BRIDGE #}
{{ casework_bridge(ref('advicepro_casework'), 'case_specific_issues_group') }}
