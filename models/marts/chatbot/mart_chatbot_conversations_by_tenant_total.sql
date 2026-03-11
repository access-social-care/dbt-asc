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
  TENANT_NAME,
  COUNT(*) AS total_conversations,
  COUNT(DISTINCT TRANSCRIPT_ID) AS unique_conversations,
  MIN(CREATED_AT) AS first_conversation_date,
  MAX(CREATED_AT) AS last_conversation_date,
  DATEDIFF('day', MIN(CREATED_AT), MAX(CREATED_AT)) AS days_active
FROM {{ source('accessava', 'accessava') }}
WHERE TENANT_NAME IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
