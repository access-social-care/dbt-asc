{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.CSI_GROUP_DIM #}
{{ casework_dim('advicepro_csi_group_bridge', 'case_specific_issues_group', 'csi_group_id') }}
