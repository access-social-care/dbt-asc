{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Contentful content metadata, one row per entry and button as landed in
  raw.contentful.contentful_data (written daily by the chatbot_content QC job;
  scripts/seed/land_contentful_raw.py is the stopgap until that job writes
  MotherDuck itself).

  Straight pass-through: lower-case column names, Snowflake types (T_* flags
  BOOLEAN, t_persona DOUBLE). No dedupe: id is NOT unique, an entry with
  several buttons has one row per button (button_id differs, all other fields
  repeat), and the rows are all distinct as landed. Consumers that need one
  row per entry take `select distinct` over the columns they use.
*/

select * from {{ source('chatbot_contentful', 'contentful_data') }}
