{{
  config(
    materialized='table',
    schema='staging_acs_helplines',
    description='Cross-source query counts aggregated to UT1/UT2 - Helplines + AdvicePro + AccessAva'
  )
}}

/*
  Cross-source staging aggregated to the UNIVERSAL THEME taxonomy (UT1/UT2)
  rather than each source's own category scheme.

  Grain: LA_NAME x MONTH_DATE x SOURCE_SYSTEM x UT1 x UT2.
  QUERY_COUNT semantics per source:
    - Helplines: pre-aggregated call count (SUM of N) - UT1 native, UT2 not captured
    - AccessAva: one count per conversation, mapped via topic_entry_point ->
      REFERENCE.TOPIC_ENTRY_POINT_MAP. NULL entry points map to 'Unmatched'
      (the map's own convention); entry points missing from the map -> 'Unmapped'.
    - AdvicePro: one count per case x topic pair via CASE_TOPIC_BRIDGE (a case
      with 3 topics contributes 3 - same convention as the segment models).
      Chain: advicepro_casework -> case_topic_bridge -> s_c_csi_map ->
      universal_themes_map (org = 'advicepro'), keyed on the canonical taxonomy:
        Community Care -> (t1 = category,      t2 = case_specific_issue)
        otherwise      -> (t1 = supercategory, t2 = category)

  'Unmapped' = the source value exists but has no row in its UT1 map (taxonomy
  drift or map gap). Kept visible rather than dropped - tests/warn_unmapped_ut1_share.sql
  warns when any source's Unmapped share exceeds threshold.
  'UNMATCHED'/'Unmatched' = the map explicitly assigns no universal theme.

  Prerequisite: REFERENCE maps reloaded with UPPERCASE columns
  (helplines_data/one_time/load_reference_maps.R) and CASE_TOPIC_BRIDGE
  loaded by advicePro_queries (PR #25).
*/

WITH helplines AS (

    SELECT
        la_name                                                    AS LA_NAME,
        DATE_TRUNC('month', month_date)::DATE                      AS MONTH_DATE,
        'Helplines'                                                AS SOURCE_SYSTEM,
        COALESCE(ut1, 'Unmapped')                                  AS UT1,
        NULL::VARCHAR                                              AS UT2,
        SUM(n)                                                     AS QUERY_COUNT
    FROM {{ source('helplines', 'helplines_aggregated_full') }}
    WHERE la_name IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5

),

accessava AS (

    -- topic_entry_point is semicolon-joined; flatten so each topic joins
    -- individually. Grain: 1 per conversation x topic (same as AdvicePro).
    -- NULL topic_entry_point -> OUTER => TRUE yields one NULL row -> 'Unmatched'.
    SELECT
        a.la_name                                                  AS LA_NAME,
        DATE_TRUNC('month', a.created_at::DATE)::DATE              AS MONTH_DATE,
        'AccessAva'                                                AS SOURCE_SYSTEM,
        CASE
            WHEN a.topic_entry_point IS NULL THEN 'Unmatched'
            ELSE COALESCE(m.ut1, 'Unmapped')
        END                                                        AS UT1,
        NULLIF(m.ut2, 'NA')                                        AS UT2,
        COUNT(*)                                                   AS QUERY_COUNT
    FROM {{ source('accessava', 'accessava') }} a,
    LATERAL FLATTEN(
        INPUT => SPLIT(a.topic_entry_point, '; '),
        OUTER => TRUE
    ) f
    LEFT JOIN {{ source('reference', 'topic_entry_point_map') }} m
        ON LOWER(TRIM(f.value::VARCHAR)) = LOWER(TRIM(m.topic_entry_point))
    WHERE a.la_name IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5

),

advicepro AS (

    SELECT
        c.la_name                                                          AS LA_NAME,
        TO_DATE(REPLACE(c.case_open_month, '/', '-') || '-01', 'YYYY-MM-DD') AS MONTH_DATE,
        'AdvicePro'                                                        AS SOURCE_SYSTEM,
        COALESCE(u.ut1, 'Unmapped')                                        AS UT1,
        NULLIF(u.ut2, 'NA')                                                AS UT2,
        COUNT(*)                                                           AS QUERY_COUNT
    FROM {{ source('casework', 'advicepro_casework') }} c
    INNER JOIN {{ source('casework', 'case_topic_bridge') }} b
        ON c.case_reference = b.case_reference
    LEFT JOIN {{ source('reference', 's_c_csi_map') }} m
        ON b.s_c_csi_id = m.s_c_csi_id
    LEFT JOIN {{ source('reference', 'universal_themes_map') }} u
        ON  u.org = 'advicepro'
        AND LOWER(TRIM(u.t1)) = LOWER(TRIM(IFF(m.supercategory = 'Community Care', m.category,            m.supercategory)))
        AND LOWER(TRIM(u.t2)) = LOWER(TRIM(IFF(m.supercategory = 'Community Care', m.case_specific_issue, m.category)))
    WHERE c.la_name IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5

)

SELECT * FROM helplines
UNION ALL
SELECT * FROM accessava
UNION ALL
SELECT * FROM advicepro
