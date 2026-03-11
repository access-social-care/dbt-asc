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
  
  **Column names verified**: tenant_name is correct.
  Table location: AVA.PUBLIC.ACCESSAVA
*/

SELECT
  tenant_name,
  DATE_TRUNC('MONTH', created_at) AS conversation_month,
  COUNT(*) AS conversation_count,
  COUNT(DISTINCT transcript_id) AS unique_conversations,
  MIN(created_at) AS first_conversation_date,
  MAX(created_at) AS last_conversation_date
FROM {{ source('accessava', 'accessava') }}
WHERE tenant_name IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, 1
