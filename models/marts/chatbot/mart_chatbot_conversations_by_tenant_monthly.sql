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
  "tenant_name" AS TENANT_NAME,
  DATE_TRUNC('MONTH', "created_at") AS CONVERSATION_MONTH,
  COUNT(*) AS CONVERSATION_COUNT,
  COUNT(DISTINCT "transcript_id") AS UNIQUE_CONVERSATIONS,
  MIN("created_at") AS FIRST_CONVERSATION_DATE,
  MAX("created_at") AS LAST_CONVERSATION_DATE
FROM {{ source('accessava', 'accessava') }}
WHERE "tenant_name" IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, 1
