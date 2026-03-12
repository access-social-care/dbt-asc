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
  "tenant_name" AS TENANT_NAME,
  COUNT(*) AS TOTAL_CONVERSATIONS,
  COUNT(DISTINCT "transcript_id") AS UNIQUE_CONVERSATIONS,
  MIN("created_at") AS FIRST_CONVERSATION_DATE,
  MAX("created_at") AS LAST_CONVERSATION_DATE,
  DATEDIFF('day', MIN("created_at"), MAX("created_at")) AS DAYS_ACTIVE
FROM {{ source('accessava', 'accessava') }}
WHERE "tenant_name" IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
