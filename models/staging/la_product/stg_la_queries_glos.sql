{{
  config(
    materialized='table',
    description='Gloucestershire-only slice of stg_la_queries — PoC output layer'
  )
}}

/*
  PoC filter: Gloucestershire only.
  All mart_glos_* models reference this instead of stg_la_queries.

  When the PoC expands to additional LAs, add their names here or promote
  to a configuration-driven filter.
*/

SELECT * FROM {{ ref('stg_la_queries') }}
WHERE LA_NAME = 'Gloucestershire'
