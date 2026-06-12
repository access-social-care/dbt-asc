{{ config(severity = 'warn') }}

/*
  Warn (not fail) when any source's 'Unmapped' UT1 share exceeds 2%.
  'Unmapped' means the source emitted a value that has no row in its UT1 map:
  taxonomy drift (new AdvicePro csi, new AccessAva topic_entry_point) or a
  map gap. Fix by extending the relevant map, not by editing the model:
    - AdvicePro: REFERENCE.S_C_CSI_MAP + advicepro rows in Universal codes.xlsx
    - AccessAva: Topic entry point map.xlsx
  then reloading via helplines_data/one_time/load_reference_maps.R.
  Surfaced by cc's Warnings section (cc PR #32) - warnings do not fail the
  pipeline but do raise a deduplicated GitHub issue.
*/

WITH shares AS (
    SELECT
        source_system,
        SUM(IFF(ut1 = 'Unmapped', query_count, 0)) AS unmapped_count,
        SUM(query_count)                           AS total_count
    FROM {{ ref('helplines_advicepro_accessava') }}
    GROUP BY source_system
)

SELECT
    source_system,
    unmapped_count,
    total_count,
    ROUND(unmapped_count / NULLIF(total_count, 0) * 100, 2) AS unmapped_pct
FROM shares
WHERE total_count > 0
  AND unmapped_count / NULLIF(total_count, 0) > 0.02
