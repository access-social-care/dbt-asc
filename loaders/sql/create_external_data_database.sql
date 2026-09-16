-- One-time setup: EXTERNAL_DATA database for the external-source pipeline
-- (see _ASC/signal_processing/README.md "External Data Pipeline" for the
-- full architecture this supports).
--
-- Run once by someone holding SYSADMIN (or an equivalently privileged role).
-- Not run by the loader itself, and not run by any agent session - this is
-- exactly the kind of schema-creating DDL this workspace's Snowflake charter
-- requires a human to approve and execute directly.
--
-- Four schemas, one per pipeline stage (see README for what belongs in each):
--   LANDING    - external_source_freshness_checker lands raw, as-published
--                rows here, append-only, one row per (dataset, vintage,
--                source row). Never queried directly by a mart.
--   RAW        - dbt staging models read LANDING and resolve it down to one
--                current row per (LA, period, measure) - the models in
--                dbt-asc/models/staging/external/.
--   NORMALISED - dbt models that divide RAW by a REFERENCE denominator
--                (population, etc.) - not yet built.
--   SIGNALS    - written back by signal_processing once it reads RAW/
--                NORMALISED and detects something - not yet built.
--
-- Adjust ROLE_DBT_TRANSFORM below if the loader/dbt connect under a
-- different role - confirm with:
--   SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
-- before running this, and check that role against what
-- loaders/load_external_sources_to_snowflake.R actually connects as
-- (ascFuncs::connect_snowflake(role = NULL) - i.e. whatever the ODBC DSN's
-- default role is, which may not be the role you're using interactively).

CREATE DATABASE IF NOT EXISTS EXTERNAL_DATA;

CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA.LANDING;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA.RAW;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA.NORMALISED;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA.SIGNALS;

-- Loader needs to create + write LANDING tables (append-only inserts).
GRANT USAGE ON DATABASE EXTERNAL_DATA TO ROLE ROLE_DBT_TRANSFORM;
GRANT USAGE, CREATE TABLE ON SCHEMA EXTERNAL_DATA.LANDING TO ROLE ROLE_DBT_TRANSFORM;

-- dbt needs to build RAW and (later) NORMALISED as models, and read LANDING.
GRANT USAGE ON SCHEMA EXTERNAL_DATA.LANDING TO ROLE ROLE_DBT_TRANSFORM;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA EXTERNAL_DATA.RAW TO ROLE ROLE_DBT_TRANSFORM;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA EXTERNAL_DATA.NORMALISED TO ROLE ROLE_DBT_TRANSFORM;

-- signal_processing will need read on RAW/NORMALISED and write on SIGNALS
-- once it moves off local files - not needed yet, included so this script
-- doesn't need a second pass when that lands. Uncomment and confirm the
-- correct role for signal_processing's own Snowflake connection first (it
-- doesn't have one today - see README "Status").
-- GRANT USAGE ON SCHEMA EXTERNAL_DATA.RAW TO ROLE <signal_processing_role>;
-- GRANT USAGE ON SCHEMA EXTERNAL_DATA.NORMALISED TO ROLE <signal_processing_role>;
-- GRANT USAGE, CREATE TABLE ON SCHEMA EXTERNAL_DATA.SIGNALS TO ROLE <signal_processing_role>;

-- Verify after running:
-- SHOW SCHEMAS IN DATABASE EXTERNAL_DATA;
-- SHOW GRANTS ON SCHEMA EXTERNAL_DATA.LANDING;
