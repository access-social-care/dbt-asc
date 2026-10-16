{{ config(materialized='table') }}
{# CASEWORK.PUBLIC.SUPER_CATEGORY_DIM #}
{{ casework_dim('advicepro_super_category_bridge', 'super_category', 'super_category_id') }}
