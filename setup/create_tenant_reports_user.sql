-- =====================================================
-- Snowflake Setup: Tenant Reports User (Read-Only)
-- =====================================================
-- Purpose: Create read-only user for tenant report generation
--          and developer access to ANALYTICS schema
--
-- Run as: ACCOUNTADMIN or SECURITYADMIN
-- When: One-time setup for tenant reporting infrastructure
-- =====================================================

USE ROLE ACCOUNTADMIN;

-- =====================================================
-- 1. CREATE ROLE: ROLE_TENANT_REPORTS_READ
-- =====================================================
-- Read-only access to ANALYTICS database
-- For: Report generators, developer access, tenant-facing APIs

CREATE ROLE IF NOT EXISTS ROLE_TENANT_REPORTS_READ
    COMMENT = 'Read-only access to ANALYTICS schema for tenant reports and developer queries';

-- Grant warehouse usage (compute resources)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ROLE_TENANT_REPORTS_READ;

-- Grant database and schema access
GRANT USAGE ON DATABASE ANALYTICS TO ROLE ROLE_TENANT_REPORTS_READ;
GRANT USAGE ON SCHEMA ANALYTICS.PUBLIC TO ROLE ROLE_TENANT_REPORTS_READ;

-- Grant SELECT on all current tables in ANALYTICS.PUBLIC
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.PUBLIC TO ROLE ROLE_TENANT_REPORTS_READ;

-- Grant SELECT on all future tables in ANALYTICS.PUBLIC (as dbt creates new marts)
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.PUBLIC TO ROLE ROLE_TENANT_REPORTS_READ;

-- Grant SELECT on all current views (if any)
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS.PUBLIC TO ROLE ROLE_TENANT_REPORTS_READ;

-- Grant SELECT on all future views
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS.PUBLIC TO ROLE ROLE_TENANT_REPORTS_READ;

-- =====================================================
-- 2. CREATE USER: TENANT_REPORTS_USER
-- =====================================================
-- Service account for tenant report generation
-- Authentication: RSA key pair (generated separately)

CREATE USER IF NOT EXISTS TENANT_REPORTS_USER
    DEFAULT_ROLE = ROLE_TENANT_REPORTS_READ
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = 'ANALYTICS.PUBLIC'
    COMMENT = 'Service account for tenant report generation and developer access to ANALYTICS schema'
    MUST_CHANGE_PASSWORD = FALSE
    RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>';  -- Replace with actual public key

-- Grant role to user
GRANT ROLE ROLE_TENANT_REPORTS_READ TO USER TENANT_REPORTS_USER;

-- =====================================================
-- 3. VERIFICATION QUERIES
-- =====================================================
-- Run these to confirm setup worked

-- Check role grants
SHOW GRANTS TO ROLE ROLE_TENANT_REPORTS_READ;

-- Check user configuration
DESC USER TENANT_REPORTS_USER;

-- Check accessible tables (switch to the new role)
USE ROLE ROLE_TENANT_REPORTS_READ;
SHOW TABLES IN SCHEMA ANALYTICS.PUBLIC;
-- Should show: MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY, MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL

-- Test query
SELECT TENANT_NAME, TOTAL_CONVERSATIONS
FROM ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL
ORDER BY TOTAL_CONVERSATIONS DESC
LIMIT 5;

-- Switch back to admin
USE ROLE ACCOUNTADMIN;

-- =====================================================
-- 4. GENERATE RSA KEY PAIR (Run locally, not in Snowflake)
-- =====================================================
/*
# On developer machine or VM

# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out tenant_reports_rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in tenant_reports_rsa_key.p8 -pubout -out tenant_reports_rsa_key.pub

# Display public key for Snowflake (remove header/footer)
grep -v "BEGIN PUBLIC KEY" tenant_reports_rsa_key.pub | grep -v "END PUBLIC KEY" | tr -d '\n'

# Store private key securely
# VM: /home/amit/.ssh/snowflake/tenant_reports_rsa_key.p8 (chmod 600)
# Dev machines: Environment variable SNOWFLAKE_TENANT_REPORTS_KEY_FILE

# Update user in Snowflake with public key:
ALTER USER TENANT_REPORTS_USER SET RSA_PUBLIC_KEY = '<paste_output_here>';
*/

-- =====================================================
-- 5. OPTIONAL: Grant to additional users
-- =====================================================
-- If individual developers need direct access (not recommended - use service account):

-- GRANT ROLE ROLE_TENANT_REPORTS_READ TO USER amit@accesscharity.org.uk;
-- GRANT ROLE ROLE_TENANT_REPORTS_READ TO USER developer@accesscharity.org.uk;

-- Note: Better practice is all access via TENANT_REPORTS_USER service account
--       with credentials distributed via environment variables

-- =====================================================
-- 6. MAINTENANCE
-- =====================================================

-- Rotate key (if compromised):
-- ALTER USER TENANT_REPORTS_USER SET RSA_PUBLIC_KEY = '<new_public_key>';

-- Disable user (if needed):
-- ALTER USER TENANT_REPORTS_USER SET DISABLED = TRUE;

-- Check who has this role:
-- SHOW GRANTS OF ROLE ROLE_TENANT_REPORTS_READ;

-- Audit user activity:
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- WHERE USER_NAME = 'TENANT_REPORTS_USER'
-- ORDER BY START_TIME DESC
-- LIMIT 100;
