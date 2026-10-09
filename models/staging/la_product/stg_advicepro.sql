{{
  config(
    materialized='table',
    description='AdvicePro cases flattened to one row per topic via case_topic_bridge, mapped to UT1/UT2'
  )
}}

/*
  Stage 1: AdvicePro with case_topic_bridge exploded.

  case_topic_bridge has one row per case x topic (pre-exploded by ETL).
  Mapping chain: case_topic_bridge -> s_c_csi_map -> universal_themes_map -> UT1/UT2.
  Community Care cases key on (category, case_specific_issue);
  all others key on (supercategory, category).

  SEGMENT = UT1 from UNIVERSAL_THEMES_MAP.
    'Unmapped' — topic exists in bridge but has no UT1 match (taxonomy drift).
    'Unmatched' — (2026-10-07) the case has no row in case_topic_bridge at all,
    i.e. no topic recorded. Same meaning as AccessAva's 'Unmatched'. Before
    2026-10-07 these were labelled 'Unmapped', which put AdvicePro at 6.21%
    Unmapped (233 of 239 were no-topic cases, 6 real drift) and made
    tests/warn_unmapped_ut1_share.sql fire on every run.
  UT2 = second-level theme from the same map (sparse; NULL where not applicable).

  Grain: one row per case x topic — except a case with zero case_topic_bridge
  rows now contributes exactly one row with UT2 = NULL (see below), not zero.
  QUERY_COUNT = 1 per row (sum gives topic mention counts, not case counts).
  HAS_LETTER = 0 — AdvicePro does not produce letters.

  CORRECTION (2026-09-25): the "Multi-Matter never explodes" diagnosis below
  is wrong. Multi-Matter is now used for ~96% of cases and the bridge DOES
  explode it (every case with CASE_SPECIFIC_ISSUES_GROUP filled is in the
  bridge, and ~99.7% of bridge rows map to a UT1). The ~1,100 cases with no
  bridge row have no topic fields in the AdvicePro report at all - see
  case_data/ADVICEPRO_MULTIMATTER.md. The LEFT JOIN is still right.

  LEFT JOIN, not INNER JOIN (2026-09-17 fix, confirmed via live investigation):
  case_topic_bridge (built by a separate repo, advicePro_queries — not this
  one) never explodes cases whose SUPER_CATEGORY = 'Multi-Matter' into topic
  rows at all, so an INNER JOIN here silently dropped every one of those
  cases entirely — 1,107 of 2,810 AdvicePro cases (39.4%), 99.6% of which
  were Multi-Matter, confirmed live 2026-09-17. Amit's direction: "the map
  DEFINITELY needs to account for the SUPERCATEGORY=MULTI-MATTER!" This
  LEFT JOIN is a MITIGATION, not the real fix — a case with no bridge match
  now lands as one row with SEGMENT = 'Unmatched' (was 'Unmapped' until
  2026-10-07) instead of vanishing silently, so it is
  at least visible and countable. It does NOT give these cases real per-topic
  UT1/UT2 classification — that requires case_topic_bridge itself to actually
  explode Multi-Matter cases, which is out of scope for this repo (needs
  investigation in advicePro_queries: whether advicepro_casework's raw
  case_specific_issues/super_category fields have usable data for Multi-Matter
  cases that the bridge ETL is just failing to explode, or whether the
  underlying data is genuinely absent for these cases — not yet answered,
  flagged as a required follow-up).
*/

SELECT
    c.la_name                                                                         AS LA_NAME,
    TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD')              AS QUERY_DATE,
    'AdvicePro'                                                                       AS SOURCE_SYSTEM,
    1                                                                                 AS QUERY_COUNT,
    CASE
        -- Test record in ADVICEPRO_CASEWORK (case_reference = 'Test', 2026-10-08):
        -- not a real case, sent to the dropped UT1. Its CSI is shared with real
        -- cases, so this cannot be done in the UT1 map.
        WHEN c.case_reference = 'Test' THEN 'UNMATCHED'
        WHEN b.case_reference IS NULL THEN 'Unmatched'
        ELSE COALESCE(u.ut1, 'Unmapped')
    END                                                                               AS SEGMENT,
    NULLIF(u.ut2, 'NA')                                                               AS UT2,
    d.age_range                                                                       AS AGE_BAND,
    0                                                                                 AS HAS_LETTER,
    loc.county                                                                        AS LOCALITY_NAME

FROM {{ source('casework', 'advicepro_casework') }} c

LEFT JOIN {{ source('casework', 'case_topic_bridge') }} b
    ON c.case_reference = b.case_reference

LEFT JOIN {{ source('reference', 's_c_csi_map') }} m
    ON b.s_c_csi_id = m.s_c_csi_id

LEFT JOIN {{ source('reference', 'universal_themes_map') }} u
    ON  u.org = 'advicepro'
    AND LOWER(TRIM(u.t1)) = LOWER(TRIM(IFF(m.supercategory = 'Community Care', m.category,            m.supercategory)))
    AND LOWER(TRIM(u.t2)) = LOWER(TRIM(IFF(m.supercategory = 'Community Care', m.case_specific_issue, m.category)))

LEFT JOIN {{ source('casework', 'advicepro_demographics') }} d
    ON c.case_reference = d.case_reference

LEFT JOIN {{ source('casework', 'casework_locality') }} loc
    ON c.case_reference = loc.case_reference

-- No filter on la_name (removed 2026-09-25, same as stg_accessava on
-- 2026-09-17): 413 of 2,819 cases have no local authority and were being
-- dropped from every downstream count, including the helplines_data SotN
-- table via helplines_advicepro_accessava.
