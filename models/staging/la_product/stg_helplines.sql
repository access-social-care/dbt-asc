{{
  config(
    materialized='table',
    description='Helplines aggregated call data — staged for LA Data Product summary mart'
  )
}}

/*
  Stage 1c: Helplines call data from HELPLINES_AGGREGATED_FULL.

  Source grain: one row per LA × month × UT1 category.
  N = pre-aggregated call count for that combination.

  SEGMENT: UT1 — the top-level helplines call category taxonomy:
    Assessments, Care plan, Carers, Charging, Direct payments,
    Information Seeking, Legal issues and complaints, Mental capacity, Safeguarding

  QUERY_COUNT = N (already aggregated, SUM in the mart gives correct totals).

  Note: Helplines does not carry locality, age band, or letter dimensions —
  these are set NULL for compatibility with the stg_la_queries interface.
*/

SELECT
    LA_NAME                                                        AS LA_NAME,
    MONTH_DATE                                                     AS QUERY_DATE,
    'Helplines'                                                    AS SOURCE_SYSTEM,
    N                                                              AS QUERY_COUNT,
    UT1                                                            AS SEGMENT,
    NULL::VARCHAR                                                  AS AGE_BAND,
    0                                                              AS HAS_LETTER,
    NULL::VARCHAR                                                  AS LOCALITY_NAME

FROM {{ source('helplines', 'helplines_aggregated_full') }}

WHERE LA_NAME IS NOT NULL
