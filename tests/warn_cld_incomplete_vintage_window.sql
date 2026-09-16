{{ config(severity = 'warn') }}

/*
  WARN when an LA does not carry the full month window for a vintage.

  Each quarterly release publishes the same rolling window of months for every
  LA - 12 for the current series. An LA with fewer months in a vintage means
  either a genuine gap at source (an LA that did not submit) or a broken
  extraction, and the two look identical downstream: a 12-month sum over that
  LA just comes out low, with nothing to signal it.

  Run against the LANDING SOURCE, not the staging models, and this is
  load-bearing rather than incidental. The models are deduped to the latest
  vintage PER PERIOD, so a model row's PUBLICATION_DATE identifies the vintage
  that won that LA-month, not the vintage's published contents: after a newer
  release lands, an older vintage retains only the months exclusive to it.
  Counting periods per publication_date in the models would therefore measure
  the dedupe's leftovers, not the window each release actually published. The
  landing table is the only place a vintage still exists intact.

  The expected window is derived per vintage as the widest month count any LA
  achieves in that vintage, rather than hardcoded to 12. Hardcoding would turn
  the first release with a different window size into a wall of false
  failures, and the check that actually matters is LA-to-LA consistency within
  a release, not the absolute number. For the current series that derived
  value should be 12 - EXPECTED_PERIODS is in the output so a wrong window is
  visible in the warning itself.

  WARN not FAIL: a real source-side gap should raise a deduplicated GitHub
  issue via cc's Warnings section, not block the pipeline for everyone else.

  !! UNVERIFIED (2026-09-16) - never executed; no Snowflake connection. The
  "widest count per vintage" assumption in particular has NOT been checked
  against real data, because no CLD CSV has been produced since the registry
  split. Re-read this test against a real vintage before trusting a green run.
*/

WITH all_landing AS (

    SELECT
        'cld_long_term_support' AS dataset_id,
        la_code,
        metric,
        _publication_date       AS publication_date,
        _source_url             AS source_url
    FROM {{ source('data_portal_landing', 'cld_long_term_support') }}

    UNION ALL

    SELECT
        'cld_assessments'       AS dataset_id,
        la_code,
        metric,
        _publication_date       AS publication_date,
        _source_url             AS source_url
    FROM {{ source('data_portal_landing', 'cld_assessments') }}

),

months_only AS (

    SELECT
        dataset_id,
        publication_date,
        source_url,
        la_code,
        metric
    FROM all_landing
    -- Total rows are not months; counting them would make every LA look like
    -- it has one period too many and mask a genuinely missing month.
    WHERE UPPER(
            TRIM(IFF(metric ILIKE '%[p]', LEFT(metric, LENGTH(metric) - 3), metric))
          ) NOT LIKE 'TOTAL%'

),

per_la AS (

    SELECT
        dataset_id,
        publication_date,
        source_url,
        la_code,
        COUNT(DISTINCT metric) AS n_periods
    FROM months_only
    GROUP BY 1, 2, 3, 4

),

expected AS (

    SELECT
        dataset_id,
        publication_date,
        source_url,
        MAX(n_periods) AS expected_periods
    FROM per_la
    GROUP BY 1, 2, 3

)

SELECT
    p.dataset_id,
    p.publication_date,
    p.source_url,
    p.la_code,
    p.n_periods,
    e.expected_periods
FROM per_la AS p
JOIN expected AS e
  ON  p.dataset_id = e.dataset_id
  AND p.publication_date IS NOT DISTINCT FROM e.publication_date
  AND p.source_url IS NOT DISTINCT FROM e.source_url
WHERE p.n_periods <> e.expected_periods
