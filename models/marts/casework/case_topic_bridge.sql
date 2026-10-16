{{ config(materialized='table') }}

/*
  DuckDB equivalent of CASEWORK.PUBLIC.CASE_TOPIC_BRIDGE: one row per distinct
  case x (csi_group, csi) pair from the pre-collapse rows, keyed by the
  deterministic s_c_csi_id of reference.s_c_csi_map. NULL s_c_csi_id means the
  pair is not in the canonical taxonomy (drift); the raw columns are the
  evidence. Cases with no topic have no rows.
*/

select
    case_reference,
    s_c_csi_id,
    csi_group_raw,
    csi_raw
from {{ ref('int_advicepro_case_topics') }}
