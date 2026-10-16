{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.CSI_DIM #}
{{ casework_dim('advicepro_csi_bridge', 'case_specific_issues', 'csi_id') }}
