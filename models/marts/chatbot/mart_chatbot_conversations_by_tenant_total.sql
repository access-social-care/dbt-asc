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
  COUNT(*) AS TOTAL_CONVERSATIONS,
  COUNT(DISTINCT TRANSCRIPT_ID) AS UNIQUE_CONVERSATIONS,
  MIN(CREATED_AT) AS FIRST_CONVERSATION_DATE,
  MAX(CREATED_AT) AS LAST_CONVERSATION_DATE,
  DATEDIFF('day', MIN(CREATED_AT), MAX(CREATED_AT)) AS DAYS_ACTIVE
FROM {{ source('accessava', 'accessava') }}
WHERE TENANT_NAME IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
