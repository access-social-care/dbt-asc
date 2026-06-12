/*
  Groundtruth: helplines_advicepro_accessava vs raw sources,
  filtered to Gloucestershire x February 2026.

  dbt analyses compile but never run automatically - execute the compiled SQL
  manually (dbt compile, then run target/compiled/.../ut1_groundtruth_feb2026_glos.sql
  block by block, or paste into a Snowflake worksheet).

  Expectations:
    1. Per-source totals in the model = raw-side totals (queries A vs B per source)
    2. No rows lost: AdvicePro bridge rows for Glos/Feb all appear (incl. Unmapped)
    3. Spot-check a few UT1 assignments against the maps by eye
*/

-- ============ A. Model-side: per-source totals and UT1 breakdown ============
SELECT source_system, ut1, ut2, SUM(query_count) AS n
FROM {{ ref('helplines_advicepro_accessava') }}
WHERE la_name = 'Gloucestershire'
  AND month_date = '2026-02-01'
GROUP BY 1, 2, 3
ORDER BY 1, n DESC;

-- ============ B1. Raw Helplines total (must equal model Helplines total) ====
SELECT ut1, SUM(n) AS n
FROM {{ source('helplines', 'helplines_aggregated_full') }}
WHERE la_name = 'Gloucestershire'
  AND DATE_TRUNC('month', month_date)::DATE = '2026-02-01'
GROUP BY 1 ORDER BY n DESC;

-- ============ B2. Raw AccessAva total (must equal model AccessAva total) ====
SELECT COUNT(*) AS conversations,
       COUNT(topic_entry_point) AS with_entry_point
FROM {{ source('accessava', 'accessava') }}
WHERE la_name = 'Gloucestershire'
  AND DATE_TRUNC('month', created_at::DATE)::DATE = '2026-02-01';

-- ============ B3. Raw AdvicePro bridge pairs (must equal model AdvicePro total)
SELECT COUNT(*) AS topic_pairs,
       COUNT(b.s_c_csi_id) AS resolved_to_taxonomy
FROM {{ source('casework', 'advicepro_casework') }} c
JOIN {{ source('casework', 'case_topic_bridge') }} b
  ON c.case_reference = b.case_reference
WHERE c.la_name = 'Gloucestershire'
  AND TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD') = '2026-02-01';

-- ============ C. Fan-out guard: UT map must be unique on its join key =======
-- Expect 0 rows. >0 means universal_themes_map has duplicate advicepro keys
-- and the model is double-counting.
SELECT LOWER(TRIM(t1)) AS k1, LOWER(TRIM(t2)) AS k2, COUNT(*) AS n
FROM {{ source('reference', 'universal_themes_map') }}
WHERE org = 'advicepro'
GROUP BY 1, 2
HAVING COUNT(*) > 1;
