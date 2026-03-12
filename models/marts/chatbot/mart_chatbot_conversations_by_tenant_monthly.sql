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
  DATE_TRUNC('MONTH', CREATED_AT) AS CONVERSATION_MONTH,
  COUNT(*) AS CONVERSATION_COUNT,
  COUNT(DISTINCT TRANSCRIPT_ID) AS UNIQUE_CONVERSATIONS,
  MIN(CREATED_AT) AS FIRST_CONVERSATION_DATE,
  MAX(CREATED_AT) AS LAST_CONVERSATION_DATE
FROM {{ source('accessava', 'accessava') }}
WHERE TENANT_NAME IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, 1
