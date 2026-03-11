{{
  config(
    materialized='table',
    description='Total conversation counts by tenant organization (all-time)'
  )
}}

/*
  Business Logic: Aggregate total chatbot conversations by tenant
  
  Owner: Product & Innovation team
  Approval required from: [PM name - update this]
  
  **IMPORTANT**: Column names need verification from actual Snowflake table.
  Replace 'organisation_name' with correct tenant column name.
  Run: SELECT * FROM AVA.ACCESSAVA.ACCESSAVA LIMIT 1
*/

SELECT
  organisation_name AS tenant_name,  -- TODO: Verify column name
  COUNT(*) AS total_conversations,
  COUNT(DISTINCT transcript_id) AS unique_conversations,
  MIN(created_at) AS first_conversation_date,
  MAX(created_at) AS last_conversation_date,
  DATEDIFF('day', MIN(created_at), MAX(created_at)) AS days_active
FROM {{ source('accessava', 'accessava') }}
WHERE organisation_name IS NOT NULL  -- TODO: Verify column name
GROUP BY 1
ORDER BY 2 DESC
