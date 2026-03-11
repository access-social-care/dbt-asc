{{
  config(
    materialized='table',
    description='Monthly conversation counts by tenant organization'
  )
}}

/*
  Business Logic: Aggregate chatbot conversations by tenant and month
  
  Owner: Product & Innovation team
  Approval required from: [PM name - update this]
  
  **IMPORTANT**: Column names need verification from actual Snowflake table.
  Replace 'organisation_name' with correct tenant column name.
  Run: SELECT * FROM AVA.ACCESSAVA.ACCESSAVA LIMIT 1
*/

SELECT
  organisation_name AS tenant_name,  -- TODO: Verify column name
  DATE_TRUNC('MONTH', created_at) AS conversation_month,
  COUNT(*) AS conversation_count,
  COUNT(DISTINCT transcript_id) AS unique_conversations,
  MIN(created_at) AS first_conversation_date,
  MAX(created_at) AS last_conversation_date
FROM {{ source('accessava', 'accessava') }}
WHERE organisation_name IS NOT NULL  -- TODO: Verify column name
GROUP BY 1, 2
ORDER BY 2 DESC, 1
