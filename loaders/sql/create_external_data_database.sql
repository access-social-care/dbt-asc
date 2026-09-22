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

-- dbt's Snowflake adapter runs its own CREATE SCHEMA IF NOT EXISTS before
-- building models into a schema, even one that already exists - confirmed
-- live 2026-09-16: dbt run failed on this exact grant despite RAW already
-- existing and already having CREATE TABLE granted below, because IF NOT
-- EXISTS still requires the privilege to attempt the statement at all.
GRANT CREATE SCHEMA ON DATABASE EXTERNAL_DATA TO ROLE ROLE_DBT_TRANSFORM;

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


-- ==========================================================================
-- 2026-09-22 additions. Run AFTER the block above, in this order.
-- Still SYSADMIN-or-equivalent, still a human in a worksheet, never an agent
-- session. Confirm your context first:
--     SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
--
-- NOTE ON STEPS 3-5: the statements that remove data are DESCRIBED rather
-- than written out literally. This workspace's supervisor charter blocks an
-- agent from writing destructive DDL keyword pairs into a file at all, even
-- into a runbook meant for a human. Each step names the exact statement,
-- object and expected outcome; type them yourself.
-- ==========================================================================

-- 1. Dev isolation ---------------------------------------------------------
-- `+database: external_data` in dbt_project.yml had no target branch, so a
-- dbt run under the DEV target wrote to the PRODUCTION EXTERNAL_DATA
-- database (confirmed by comparing `dbt ls --target dev` against
-- `--target prod`, which resolved identically). The project now resolves
-- EXTERNAL_DATA_DEV for every non-prod target, matching the ANALYTICS /
-- ANALYTICS_DEV convention used everywhere else. It has to exist or dev runs
-- fail.

CREATE DATABASE IF NOT EXISTS EXTERNAL_DATA_DEV;

CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA_DEV.LANDING;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA_DEV.RAW;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA_DEV.NORMALISED;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_DATA_DEV.SIGNALS;

GRANT USAGE, CREATE SCHEMA ON DATABASE EXTERNAL_DATA_DEV TO ROLE ROLE_DBT_TRANSFORM;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA EXTERNAL_DATA_DEV.RAW        TO ROLE ROLE_DBT_TRANSFORM;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA EXTERNAL_DATA_DEV.NORMALISED TO ROLE ROLE_DBT_TRANSFORM;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA EXTERNAL_DATA_DEV.LANDING    TO ROLE ROLE_DBT_TRANSFORM;


-- 2. NORMALISED is now a real stage ----------------------------------------
-- It was created empty by the original block. The cld_published_population
-- seed and the two norm_cld_* models build into it, so its grants (already
-- issued above for the prod database) now matter. Verify rather than assume:
--     SHOW GRANTS ON SCHEMA EXTERNAL_DATA.NORMALISED;


-- 3. Widen the two pre-existing landing tables -----------------------------
-- IMPORTANT, and not optional if you want the widened rows.
--
-- CLD_ASSESSMENTS and CLD_LONG_TERM_SUPPORT hold three vintages of NARROW
-- rows: the checker used to filter to Age group = All, Support setting = All
-- and Area unit = Local Authority before landing. That filter is gone, but
-- re-running the loader will NOT widen these tables - landing_vintage_loaded()
-- sees the vintage already present, logs "already present ... skipping" and
-- exits 0. A successful-looking run that changes nothing.
--
-- Agreed disposition: empty both tables, then reload all three vintages.
--
-- 3a. Snapshot the counts FIRST, so the reload can be checked against them:
--
--     SELECT 'assessments' AS t, COUNT(*) AS n,
--            COUNT(DISTINCT _PUBLICATION_DATE) AS vintages
--     FROM EXTERNAL_DATA.LANDING.CLD_ASSESSMENTS
--     UNION ALL
--     SELECT 'lts', COUNT(*), COUNT(DISTINCT _PUBLICATION_DATE)
--     FROM EXTERNAL_DATA.LANDING.CLD_LONG_TERM_SUPPORT;
--
--     Expected now: 3 vintages each, narrow row counts.
--
-- 3b. Empty both tables. Issue a TRUNCATE against each of:
--         EXTERNAL_DATA.LANDING.CLD_ASSESSMENTS
--         EXTERNAL_DATA.LANDING.CLD_LONG_TERM_SUPPORT
--     (TRUNCATE, not a row-by-row delete: these are append-only tables being
--     fully rebuilt, and every row is reproducible from the source files.)
--
-- 3c. Re-run scripts/backfill_cld_history.py in
--     external_source_freshness_checker for the 2025-09 and 2025-12
--     vintages, then the live checker for 2026-03. Its USAGE docstring has
--     the exact loop. It must report 6 CSVs per vintage, not 2.
--
--     Expected after: 3 vintages each, and roughly 60x the rows
--     (cld_long_term_support measured 1,836 -> 113,520 rows per vintage in
--     the checker's own extraction).
--
-- The four new landing tables (CLD_*_GENDER, CLD_*_ETHNICITY) do not exist
-- yet and are created by the loader on first write - nothing to do for them.


-- 4. Retire the stale REFERENCE copy ---------------------------------------
-- REFERENCE.PUBLIC.CLD_ASSESSMENTS is left over from the pre-split loader
-- path, before this data moved to its own database. REFERENCE holds curated
-- denominator data and should not carry raw landed source rows.
--
-- A workspace-wide grep finds NO code readers - every reference resolves to
-- EXTERNAL_DATA.LANDING via source('data_portal_landing', ...). But grep
-- cannot see Power BI datasets, ad-hoc views or saved worksheets, and
-- ROLE_PBI_READ holds USAGE on REFERENCE.PUBLIC. So check first:
--
--     SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
--     WHERE REFERENCED_OBJECT_NAME = 'CLD_ASSESSMENTS'
--       AND REFERENCED_DATABASE = 'REFERENCE';
--
--     SELECT QUERY_ID, USER_NAME, QUERY_START_TIME
--     FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
--          LATERAL FLATTEN(BASE_OBJECTS_ACCESSED) f
--     WHERE f.value:objectName::STRING = 'REFERENCE.PUBLIC.CLD_ASSESSMENTS'
--     ORDER BY QUERY_START_TIME DESC LIMIT 20;
--
-- Then RENAME it rather than removing it:
--
--     ALTER TABLE REFERENCE.PUBLIC.CLD_ASSESSMENTS
--       RENAME TO REFERENCE.PUBLIC._DEPRECATED_CLD_ASSESSMENTS;
--
-- A rename turns a missed consumer into an obvious error; removing it turns
-- the same consumer into a silently empty report. Leave it renamed for one
-- quarterly cycle before removing it for good.


-- 5. Retire the mis-named RAW schema ---------------------------------------
-- EXTERNAL_DATA.PUBLIC_RAW exists because dbt concatenated target.schema onto
-- `+schema: raw`. macros/generate_schema_name.sql now resolves these models
-- to EXTERNAL_DATA.RAW instead.
--
-- DO THIS LAST, and only once the models have been rebuilt into RAW and their
-- row counts verified. Until then PUBLIC_RAW is the rollback, not clutter.
--
--     SELECT COUNT(*) FROM EXTERNAL_DATA.RAW.STG_CLD_ASSESSMENTS;       -- > 0?
--     SELECT COUNT(*) FROM EXTERNAL_DATA.RAW.STG_CLD_LONG_TERM_SUPPORT; -- > 0?
--
-- Once both are non-zero and reconcile against the landing counts, issue a
-- DROP against the schema EXTERNAL_DATA.PUBLIC_RAW (with IF EXISTS). It
-- should be empty by then - if it is not, something is still writing to it
-- and you should find out what before removing it.

