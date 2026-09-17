{{ config(severity = 'warn') }}

/*
  Warn (not fail) when any source's 'Unmapped' UT1 share exceeds 2%.
  'Unmapped' means the source emitted a value that has no row in its UT1 map:
  taxonomy drift (new AdvicePro csi, new AccessAva topic_entry_point) or a
  map gap. Fix by extending the relevant map, not by editing the model:
    - AdvicePro: REFERENCE.S_C_CSI_MAP + advicepro rows in Universal codes.xlsx
    - AccessAva: Topic entry point map.xlsx
  then reloading via helplines_data/one_time/load_reference_maps.R.

  AdvicePro 'Unmapped' now has a SECOND, larger cause as of 2026-09-17:
  stg_advicepro.sql's join to case_topic_bridge changed from INNER to LEFT
  (see that model's header comment) so a case with no case_topic_bridge row
  at all - overwhelmingly SUPER_CATEGORY='Multi-Matter' cases, which the
  bridge-building ETL in advicePro_queries never explodes - now lands here as
  'Unmapped' too, instead of being silently dropped. Expect AdvicePro's
  Unmapped share to sit well above the 2% threshold below until that's fixed
  at the source (tracked as a follow-up in advicePro_queries, not this repo).
  Extending REFERENCE.S_C_CSI_MAP will NOT fix that portion - there is no
  taxonomy row missing, there is no bridge row to map in the first place.
  If this test fires for AdvicePro, check the Multi-Matter share of the
  Unmapped rows before assuming it's ordinary taxonomy drift.

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
