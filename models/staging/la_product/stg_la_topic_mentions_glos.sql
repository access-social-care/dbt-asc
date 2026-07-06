{{
  config(
    materialized='table',
    description='Gloucestershire-only slice of stg_la_topic_mentions — topic-mention grain, all three sources'
  )
}}

/*
  PoC filter: Gloucestershire only.
  All int_glos_* and mart_glos_* models reference this.

  When the PoC expands to additional LAs, add their names here or promote
  to a configuration-driven filter.
*/

SELECT * FROM {{ ref('stg_la_topic_mentions') }}
WHERE LA_NAME = 'Gloucestershire'
