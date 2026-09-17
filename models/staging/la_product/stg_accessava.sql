{{
  config(
    materialized='table',
    description='AccessAva conversations flattened to one row per topic_entry_point mention, mapped to UT1/UT2'
  )
}}

/*
  Stage 1: AccessAva with topic_entry_point flattened.

  topic_entry_point is semicolon-space-joined (e.g. "Housing; Benefits; Legal").
  Each conversation expands to one row per topic.

  SEGMENT = UT1 from TOPIC_ENTRY_POINT_MAP.
    'Unmatched' — topic_entry_point was NULL.
    'Unmapped'  — value exists but has no row in the map (taxonomy drift).
  UT2 = second-level theme from the same map (sparse; NULL where not applicable).

  Grain: one row per conversation x topic.
  QUERY_COUNT = 1 per row (sum gives topic mention counts, not conversation counts).

  LA_NAME MAY BE NULL (2026-09-17 fix, confirmed via live investigation): the
  raw source's la_name is NULL for the large majority of AccessAva conversations
  (87.5% overall as of 2026-09-17, worsening over time — 96-98% in the most
  recent 6 months, concentrated in specific high-volume tenants). This model
  used to silently drop every one of those rows via `WHERE la_name IS NOT NULL`
  in the source subquery below — Amit's explicit direction was to stop doing
  that and keep the data ("remove the data drop where la_name is null! I want
  to keep all that data please!"). NULL LA_NAME rows now flow through the full
  chain: into stg_la_topic_mentions, into helplines_advicepro_accessava's
  GROUP BY LA_NAME (NULL forms its own group there, same as any other SQL
  GROUP BY), and into helplines_data's SOTN build (see that model's header
  comment) — SOTN's England-only LA_CODE filter and its LA_NAME/LA_CODE
  reference check both already special-case AccessAva/AdvicePro rows (they
  never carry LA_CODE from this source), so this change does not interact
  badly with either downstream check. Confirmed directly in
  helplines_data/one_time/build_sotn_table.R rather than assumed. Because
  LA_NAME is no longer guaranteed non-null, the `not_null` test on this
  column was removed from schema.yml (see that file's comment).
*/

SELECT
    a.la_name                                                               AS LA_NAME,
    a.created_at::DATE                                                      AS QUERY_DATE,
    'AccessAva'                                                             AS SOURCE_SYSTEM,
    1                                                                       AS QUERY_COUNT,
    CASE
        WHEN a.topic_entry_point IS NULL THEN 'Unmatched'
        ELSE COALESCE(m.ut1, 'Unmapped')
    END                                                                     AS SEGMENT,
    NULLIF(m.ut2, 'NA')                                                     AS UT2,
    a.age                                                                   AS AGE_BAND,
    CASE WHEN a.lettercode IS NOT NULL THEN 1 ELSE 0 END                    AS HAS_LETTER,
    l.county                                                                AS LOCALITY_NAME

FROM (
    SELECT
        la_name,
        created_at,
        age,
        lettercode,
        transcript_id,
        topic_entry_point,
        TRIM(f.value::VARCHAR)                                              AS topic_value
    FROM {{ source('accessava', 'accessava') }},
    LATERAL FLATTEN(
        INPUT  => SPLIT(topic_entry_point, '; '),
        OUTER  => TRUE
    ) f
    -- No la_name filter here (removed 2026-09-17) — see header comment.
    -- NULL la_name rows are kept, not dropped.
) a
LEFT JOIN {{ source('accessava', 'accessava_locality') }} l
    ON a.transcript_id = l.transcript_id
LEFT JOIN {{ source('reference', 'topic_entry_point_map') }} m
    ON LOWER(a.topic_value) = LOWER(TRIM(m.topic_entry_point))
