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
  
  **Column names verified**: tenant_name is correct.
  Table location: AVA.PUBLIC.ACCESSAVA
*/

SELECT
  tenant_name,
  COUNT(*) AS total_conversations,
  COUNT(DISTINCT transcript_id) AS unique_conversations,
  MIN(created_at) AS first_conversation_date,
  MAX(created_at) AS last_conversation_date,
  DATEDIFF('day', MIN(created_at), MAX(created_at)) AS days_active
FROM {{ source('accessava', 'accessava') }}
WHERE tenant_name IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
