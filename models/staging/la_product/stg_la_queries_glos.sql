{{
  config(
    materialized='table',
    description='Gloucestershire-only slice of stg_la_queries_segments — topic-mention grain, all three sources'
  )
}}

/*
  PoC filter: Gloucestershire only.
  All mart_glos_* models reference this instead of stg_la_queries.

  When the PoC expands to additional LAs, add their names here or promote
  to a configuration-driven filter.
*/

SELECT * FROM {{ ref('stg_la_queries_segments') }}
WHERE LA_NAME = 'Gloucestershire'
