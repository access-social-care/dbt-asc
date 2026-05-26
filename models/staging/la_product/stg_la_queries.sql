{{
  config(
    materialized='table',
    description='Combined LA query data: AccessAva + AdvicePro (UNION ALL of staging models)'
  )
}}

/*
  Stage 2: UNION ALL of pre-staged source models.
  Each source model normalises to the same column interface:
    - stg_advicepro  → AdvicePro cases (casework + demographics + locality joined)
    - stg_accessava  → AccessAva chatbot conversations

  All macros in models/marts/la_product/ reference this model (or stg_la_queries_glos for PoC).
  Helplines excluded from PoC — re-add when UT1 → shared segment taxonomy is agreed.
*/

SELECT * FROM {{ ref('stg_advicepro') }}
UNION ALL
SELECT * FROM {{ ref('stg_accessava') }}
