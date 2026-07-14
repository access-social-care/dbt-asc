{{
  config(
    materialized='table',
    description='PoC: dbt-generated shadow of AVA.PUBLIC.PERSONA_DIM'
  )
}}

/*
  Proof-of-concept companion to persona_bridge_dbt.sql. See that model's
  header comment for context.

  Grain: one row per distinct persona value, with a stable surrogate key
  assigned by alphabetical order (same convention as add_surrogate_key() in R).
*/

SELECT
    ROW_NUMBER() OVER (ORDER BY PERSONA)  AS PERSONA_ID,
    PERSONA
FROM (
    SELECT DISTINCT PERSONA
    FROM {{ ref('persona_bridge_dbt') }}
) d
