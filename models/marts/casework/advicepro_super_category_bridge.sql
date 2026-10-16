{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.SUPER_CATEGORY_BRIDGE. Prefixed advicepro_ because marts.main
   already holds the chatbot csi_bridge / csi_dim. #}
{{ casework_bridge(ref('advicepro_casework'), 'super_category') }}
