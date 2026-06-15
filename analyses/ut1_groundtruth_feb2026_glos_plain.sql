/*
  Groundtruth: helplines_advicepro_accessava vs raw sources,
  Gloucestershire x February 2026.

  PLAIN SQL - no dbt Jinja. Paste each block into a Snowflake worksheet.
  Run after dbt build has materialised the model.

  Fully-qualified names (post-dbt build):
    Model:   ANALYTICS.PUBLIC_STAGING_ACS_HELPLINES.HELPLINES_ADVICEPRO_ACCESSAVA
    Sources: HELPLINES.PUBLIC.HELPLINES_AGGREGATED_FULL
             AVA.PUBLIC.ACCESSAVA
             REFERENCE.PUBLIC.TOPIC_ENTRY_POINT_MAP
             CASEWORK.PUBLIC.ADVICEPRO_CASEWORK
             CASEWORK.PUBLIC.CASE_TOPIC_BRIDGE
             REFERENCE.PUBLIC.S_C_CSI_MAP
             REFERENCE.PUBLIC.UNIVERSAL_THEMES_MAP

  Expectations:
    A: per-source totals + UT1 breakdown from the materialised model
    B1: Helplines raw total = model Helplines total
    B2: AccessAva raw conversation count (for manual cross-check)
    B3: AdvicePro raw bridge pairs = model AdvicePro total
    C:  Fan-out guard on universal_themes_map (expect 0 rows)
*/

USE ROLE ROLE_DBT_TRANSFORM;

-- ============ A. Model-side: per-source totals and UT1 breakdown ============
SELECT source_system, ut1, ut2, SUM(query_count) AS n
FROM ANALYTICS.PUBLIC_STAGING_ACS_HELPLINES.HELPLINES_ADVICEPRO_ACCESSAVA
WHERE la_name = 'Gloucestershire'
  AND month_date = '2026-02-01'
GROUP BY 1, 2, 3
ORDER BY 1, n DESC;

-- ============ B1. Raw Helplines total (must equal model Helplines total) ====
SELECT ut1, SUM(n) AS n
FROM HELPLINES.PUBLIC.HELPLINES_AGGREGATED_FULL
WHERE la_name = 'Gloucestershire'
  AND DATE_TRUNC('month', month_date)::DATE = '2026-02-01'
GROUP BY 1 ORDER BY n DESC;

-- ============ B2. Raw AccessAva total (must equal model AccessAva total) ====
SELECT COUNT(*) AS conversations,
       COUNT(topic_entry_point) AS with_entry_point
FROM AVA.PUBLIC.ACCESSAVA
WHERE la_name = 'Gloucestershire'
  AND DATE_TRUNC('month', created_at::DATE)::DATE = '2026-02-01';

-- ============ B3. Raw AdvicePro bridge pairs (must equal model AdvicePro total)
SELECT COUNT(*) AS topic_pairs,
       COUNT(b.s_c_csi_id) AS resolved_to_taxonomy
FROM CASEWORK.PUBLIC.ADVICEPRO_CASEWORK c
JOIN CASEWORK.PUBLIC.CASE_TOPIC_BRIDGE b
  ON c.case_reference = b.case_reference
WHERE c.la_name = 'Gloucestershire'
  AND TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD') = '2026-02-01';

-- ============ C. Fan-out guard: UT map must be unique on its join key =======
-- Expect 0 rows. >0 means universal_themes_map has duplicate advicepro keys
-- and the model is double-counting.
SELECT LOWER(TRIM(t1)) AS k1, LOWER(TRIM(t2)) AS k2, COUNT(*) AS n
FROM REFERENCE.PUBLIC.UNIVERSAL_THEMES_MAP
WHERE org = 'advicepro'
GROUP BY 1, 2
HAVING COUNT(*) > 1;
