{{
  config(
    materialized='table',
    description='PoC: dbt-generated shadow of AVA.PUBLIC.PERSONA_BRIDGE'
  )
}}

/*
  Proof-of-concept: reproduce ascFuncs::make_bridge_dim()'s PERSONA_BRIDGE
  output entirely in dbt/SQL, to test whether bridge/dim generation can
  move out of R and into dbt for tables derived purely from ACCESSAVA.

  Suffixed _dbt so this sits alongside the production AVA.PUBLIC.PERSONA_BRIDGE
  table (built by R) for row-level diffing, not replacing it.

  Grain: one row per transcript_id x persona value (persona is semicolon-joined
  in the source, split here same as split_to_long() does in R).
*/

SELECT
    transcript_id                    AS TRANSCRIPT_ID,
    persona_value                    AS PERSONA
FROM (
    {{ split_to_rows(source('accessava', 'accessava'), 'persona', ';',
                     ['transcript_id', 'persona'], 'persona_value') }}
) s
WHERE persona IS NOT NULL
  AND persona_value != ''
