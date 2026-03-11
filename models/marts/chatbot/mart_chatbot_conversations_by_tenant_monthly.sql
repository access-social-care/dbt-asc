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
  TENANT_NAME,
  DATE_TRUNC('MONTH', CREATED_AT) AS conversation_month,
  COUNT(*) AS conversation_count,
  COUNT(DISTINCT TRANSCRIPT_ID) AS unique_conversations,
  MIN(CREATED_AT) AS first_conversation_date,
  MAX(CREATED_AT) AS last_conversation_date
FROM {{ source('accessava', 'accessava') }}
WHERE TENANT_NAME IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, 1
