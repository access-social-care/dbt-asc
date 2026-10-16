# dbt-asc Charter

> Operational docs (setup, loaders, models, cron entry): [README.md](README.md)

## What this is

dbt transformation layer for Access Social Care's Snowflake data warehouse. Takes raw
data from three upstream databases (AVA, CASEWORK, HELPLINES) and produces
`ANALYTICS.PUBLIC` - the single schema consumed by Power BI and web products. Also
contains the R loaders that populate `CASEWORK.PUBLIC` from AdvicePro and Monday.com.

---

## Schema invariants

Three raw databases feed dbt:

| Database | Loaded by | Schedule |
|----------|-----------|----------|
| `AVA.PUBLIC` | `chatbot_data` repo | Daily ~05:00 |
| `CASEWORK.PUBLIC` | `loaders/load_primary_data.sh` (this repo) | Daily 06:00 |
| `HELPLINES.PUBLIC` | `helplines_data` repo | Monthly, manual |

**Web products and Power BI connect to `ANALYTICS.PUBLIC` only.** They should never
query `AVA`, `CASEWORK`, or `HELPLINES` directly. If a downstream tool appears to be
missing data, check `ANALYTICS.PUBLIC` first before touching raw schemas.

---

## `la_suppress()` changes output column type

`la_suppress(expr)` converts any count below 5 to the string `'1-5'`. **Output columns
using `la_suppress` are VARCHAR, not numeric.** Power BI must treat these columns as
text (not integers). Do not cast them to INT in Power BI transformations - a cell
containing `'1-5'` will become an error.

---

## `ROLE_PBI_READ` seeing raw, unsuppressed data is intentional

`ROLE_PBI_READ` has blanket `SELECT` on `ANALYTICS.PUBLIC` (see `setup/snowflake_permissions.sql`),
which includes unsuppressed counts. **This is by design, not a gap.** Power BI is an
internal-only tool - staff are allowed to see raw numbers. `la_suppress()` exists to protect
the Gloucestershire LA product's *external*-facing output, not anything Power BI touches.
Do not read PBI_READ's raw-data access as a disclosure-control failure.

**Separate, unresolved question:** the Gloucestershire product's own schemas
(`ANALYTICS.PUBLIC_LA_PRODUCT` for `mart_glos_*`, `ANALYTICS.PUBLIC_LA_PRODUCT_STAGING` for
`int_glos_*` - physical names per dbt's default `generate_schema_name`, no override macro exists
in this repo) have **no `GRANT` statements for either schema anywhere in this repo** -
`setup/snowflake_permissions.sql` only grants `ROLE_PBI_READ` and `ROLE_DBT_TRANSFORM` access to
`ANALYTICS.PUBLIC`, `AVA.PUBLIC`, `CASEWORK.PUBLIC`, and `REFERENCE.PUBLIC`. Since Snowflake
schema grants don't cascade from one schema to a sibling, this means who can actually read
`PUBLIC_LA_PRODUCT`/`PUBLIC_LA_PRODUCT_STAGING` - and specifically whether anything reaches the
unsuppressed `int_glos_*` layer - is not answerable from this repo. Either the grants exist,
applied directly in Snowflake and never captured here, or they don't exist yet. Worth confirming
directly in Snowflake (`SHOW GRANTS ON SCHEMA ANALYTICS.PUBLIC_LA_PRODUCT_STAGING`) rather than
assuming either way.

---

## Loader phase ordering

`loaders/load_primary_data.sh` runs in two phases:

1. Phase 1: source system loads (no dependencies) - AdvicePro demographics, Monday.com
2. Phase 2: derived loads (must run after phase 1) - postcode→LA lookup

Phase 2 reads postcode data written by Phase 1. Running Phase 2 alone produces empty
or stale locality joins. The orchestration in `run_pipeline.sh` enforces this order -
do not run phases out of order.

---

## Postcode lookup invariant

AdvicePro stores cases with client postcodes, not LA names. The locality loader
(Phase 2) resolves postcodes to LA names via `findthatpostcode.uk`. AccessAva already
knows the deploying tenant's LA - no postcode lookup is needed for chatbot data.
Do not add a postcode lookup step for chatbot records.

---

## Every AdvicePro S/C/CSI must map to a UT1

Every (supercategory, category, case-specific-issue) combination in
`REFERENCE.PUBLIC.S_C_CSI_MAP` must resolve to a UT1 in
`REFERENCE.PUBLIC.UNIVERSAL_THEMES_MAP` (org = `advicepro`). An explicit
`UNMATCHED` UT1 counts as a decision; a missing row does not. A case that lands
in `stg_advicepro` as `Unmapped` is therefore a map gap to fix, never
acceptable steady state: add the row to `helplines_data/intermediate/Universal
codes.csv` (synced to Snowflake every pipeline run), or the CSI to
`S_C_CSI_MAP`. `tests/warn_unmapped_ut1_share.sql` is the tripwire.

A case with no `case_topic_bridge` row (no topic recorded at source) is
`Unmatched`, not `Unmapped`: that is missing source data, not a missing map row,
and no mapping change recovers it.

---

## One AdvicePro case = one person

Every AdvicePro case is **one person**. No case has more than one human attached. Counting
cases is counting people.

**Why a report can show more rows than cases.** AdvicePro's "What type of disability" question
is multi-select, so equalities reports list a case once per type ticked. Checked 2026-10-13
for Apr-Sep 2026: the per-case report has 214 rows but only 187 distinct cases (186 once the
test case `Test4` is removed); the 27 extra rows are 9 cases with two types, 6 with three and
2 with four. The casework table and the annual AdvicePro report agree on the 187.

What this means for the models:
- Count **distinct case references**, never report rows. A person with several types ticked
  is one person: `stg_ilta_advicepro` puts them under `Other` for disability.
- The loader for report `FD7DXGL4` collapses the repeated rows to one row per case and joins
  the types with `;`. FD7DXGL4 only has cases where something was filled in, but it holds every
  answered value (it reproduces the full per-case report line for line); casework is the
  denominator.
- A version of the equalities report **without a Case Reference column** (the API run of
  `HS6T5CH9`) cannot be de-duplicated: it merges identical blank rows into one row with a
  "Number of Person with Care Needs" count (not a headcount) and still repeats multi-type
  cases. Do not use it for counts.
- AdvicePro forces a Yes/No on "Do you have a disability", so a `No` with nothing else
  answered is a default, not a response (155 of 161 `No` in Apr-Sep 2025). ILTA models treat
  it as `Not recorded`.
- AdvicePro test cases (`Test1`, `Test3`, `Test4`...) exist in `ADVICEPRO_CASEWORK`; exclude
  them from any count.

---

## `stg_la_queries` is the integration point

`stg_la_queries.sql` is the single UNION ALL that combines AdvicePro cases and
AccessAva conversations into one row shape. All 7 LA product mart models read from
this staging model, not directly from raw tables. Changes to mart model behaviour
start here.

---

## `mart_chatbot_*` vs LA product marts

Two `mart_chatbot_*` models exist for internal chatbot team use (conversation counts
by tenant, monthly and all-time). These are separate from the 7 LA product models -
different consumers, different grain. Do not conflate them.

---

## Package updates require manual `dbt deps`

If `packages.yml` changes (new dbt package added), run `dbt deps` manually on the VM
before the next cron run. It is not part of the daily pipeline. The cron will fail
with a missing package error if this step is skipped.

---

## DuckDB / MotherDuck target (experiment; Snowflake stays live as fallback)

Decisions only. Setup and commands: [profiles/profiles.yml](profiles/profiles.yml).

- One dbt project, two targets. Runs are selected by tag (`external`, `chatbot`, `casework`,
  `helplines`, `wisetime`, `cross_source`), never by forking models.
- Every command carries `--profiles-dir profiles --target duckdb`. The default target in
  `~/.dbt` is Snowflake prod, so a bare run would write to prod.
- Layers are MotherDuck DATABASES (owner convention), schema `main`: `raw` and `raw_external`
  (inputs, including anything we land, e.g. `raw.contentful`, `raw.chatbot`), `staging`
  (staging, intermediate, seeds, dbt tests), `marts` (marts and normalised), `reference`
  (input). dbt writes only `staging` and `marts`. Snowflake layers, roles and envs are not
  recreated. Connection: `path: md:staging` plus `attach: md:marts` in
  [profiles/profiles.yml](profiles/profiles.yml) (`path: md:` alone is rejected by dbt-duckdb).
- `generate_database_name` (new) routes by folder: marts/normalised models -> `marts`, all else
  -> `staging`; non-duckdb returns dbt's default exactly. `generate_schema_name`'s duckdb branch
  returns `main`, both ahead of the Snowflake logic. The Snowflake
  branches is a grant boundary and stays byte-identical; verify any edit with
  `dbt ls --target prod --output json --output-keys name schema database resource_type`
  before and after (diff must be empty).
- Sources are target-conditional: accessava -> `raw.aa_postgres_public`, helplines ->
  `raw.share_point` (one table per partner file), reference -> `reference.main`, external ->
  `raw_external.main`.
- Not on MotherDuck yet, so disabled on duckdb (`enabled: target.type != 'duckdb'`) rather than
  half-ported: AdvicePro casework, WiseTime, and every model that depends on them
  (`casework`, `cross_source` tags: la_product, ilta marts, `helplines_advicepro_accessava`).
  `stg_cld_*` are disabled too: the landing tables are absent.
- `raw_external.main.cld_*` already holds the typed, deduped `stg_cld_*` output (parity-checked
  against `EXTERNAL_DATA.RAW`), so on duckdb `norm_cld_*` read it directly via `cld_raw_relation`.
- `accessava` and `accessava_locality` do not exist in `raw.aa_postgres_public` (Snowflake builds
  them in the R chatbot loader from the Postgres tables). The chatbot models need them built
  in SQL before they can run on duckdb.
- Snowflake-only SQL goes through cross-adapter macros (`split_to_rows`) so the Snowflake
  compiled path is unchanged. `::FLOAT` became `::DOUBLE` (identical type on Snowflake, but
  32-bit on DuckDB, which broke rate parity).
- Parity rule: DuckDB values equal Snowflake unless a bug is found. Document the bug; fix it on
  the DuckDB side only if it is clearly a bug. Checked with `scripts/parity/parity.py`
  (Snowflake read-only); summaries in `docs/parity/`. Tolerances: float rel 1e-9, timestamps to
  the second in UTC, column names case-insensitive, `''` vs NULL reported separately.

### Rules carried in model code (stated so a port keeps them)

- AccessAva `topic_entry_point` is a `'; '`-delimited tag list; one row per topic, a NULL list
  keeps one row. NULL topic -> `Unmatched`; topic missing from `topic_entry_point_map` ->
  `Unmapped`. Map join is case-insensitive on trimmed text; UT2 `'NA'` means NULL.
- Rows with NULL `la_name` are kept in `stg_accessava` (deliberate, 2026-09-17).
- Persona is a `';'`-delimited tag list: one bridge row per transcript and trimmed non-empty
  value; `persona_dim_dbt` ids are alphabetical `ROW_NUMBER`.
- CLD: publisher "Total" metric rows are excluded; suppressed `[c]` cells stay NULL, never 0;
  duplicate vintages resolve to the latest `publication_date` (text, `YYYY-MM`), then `run_at`;
  the dedupe key uses `COALESCE(area_code, area_name)` because ADASS Region rows have no code.
- CLD rates are monthly counts over frozen published denominators (inner join, so areas with no
  denominator are dropped), rounded to 2 dp.

### Port decisions: chatbot, helplines, external (2026-10)

Apply once the matching `feat/duckdb-*` branches merge. Where they contradict the DuckDB
section above (stg_cld_* disabled, `cld_raw_relation`, accessava missing), these win.

Chatbot
- The R chatbot extractors (`ascFuncs` `chatbot_*`) are replaced by one dbt model per function,
  keyed on `transcript_id`. `stg_chatbot_transcript_event` is the `tr_ev` equivalent and is
  rebuilt from the Fivetran copy in `raw.aa_postgres_public` on every run. The id-paged
  downloads, `.rds`/`.fst` caches, `last_id_*.txt` position files and `data.csv` are retired.
- Rendering `report.Rmd` and exporting `data.csv` were one job. In dbt they are separate.
- Test users are removed by two independent rules, as in R: the tracker `user_input` name for
  transcripts, and the answer to "What should I call you?" for events.
- Fivetran soft deletes are dropped before the latest row per id is chosen, so a live row with a
  newer deleted twin is kept.
- `is_reporting_tenant` flags UAT and APDAS_SQUARESPACE tenants as non-reporting instead of
  deleting them; ACCESSAVA filters on it.
- One conversation may have several personas (`'1; 3'`, first-seen order). Delimiters follow R:
  `'; '` for persona, topic_entry_point, supercategories, categories, csis; `';'` for lettercode
  and consumed_resources. An answer of the text `NA` is NULL; mixed values are kept.
- ethnicity and age_range keep the latest answer per transcript so `transcript_id` stays unique.
- Mirrored R quirks, kept for parity until the owner decides: `person_type` holds the bot's
  follow-up message because R sorts descending before `lag()` (var
  `chatbot_person_type_fix_order` gives the real answers); `feedback` compares across
  transcripts; `age` keeps the last distinct answer in first-seen order.
- Tracker-derived flags read the live tracker and can be TRUE where the old R snapshot was NULL.
  Only about 445 transcripts (2014 to 2023) have a tracker, so newer data cannot change.
- Contentful metadata is a snapshot seeded from Snowflake `CONTENTFUL_DATA` into
  `analytics_dev.landing`. Snowflake cannot be retired for chatbot until Contentful lands in
  MotherDuck natively.
- The LA-to-CASSR map, tenant-to-LA map and category map are static copies of `ascFuncs` data.
- Form demographics (170 transcripts, `demographics_matched.RDS`) are loaded by script into
  `landing`, never committed: PII-adjacent. They should land in MotherDuck natively, as should
  Contentful; the seed scripts then go away.
- ACCESSAVA is one row per reporting-tenant `transcript_id`, built in dbt from the `int_chatbot_*`
  models. `data.csv`, the `report.Rmd` export step and `data_uploader.R` are retired for it.
- The `report.Rmd` replacement of `'; '` with `,` and the persona relabel apply only to the
  Monday export and never reach ACCESSAVA.
- Every bridge splits on `;` with optional surrounding whitespace and squishes tokens. Bridges are
  not distinct, as in R. Dim keys are a binary-order sort numbered from 1. `SUPERCATEGORY_DIM`
  keeps the literal `NA`. Dim and bridge integrity is dbt tests (unique, not_null, relationships).
- `add_surrogate_key` captures its argument by name (`ensym`), so the persona call that passes
  the whole list is correct, not a bug.
- `ACCESSAVA_LOCALITY` stands alone: first row per transcript in Postgres physical order (Fivetran
  ctid). `county` is NULL for every ACCESSAVA row and stays NULL.
- Fivetran keys `actions_transcript` by ctid, so an update lands as a live row plus a soft-deleted
  twin. Soft-deleted rows are dropped first, then the latest live row per id is kept.
- `accessava_relation()` points downstream models at the dbt ACCESSAVA on duckdb and the Snowflake
  source otherwise.

Helplines
- Partner files arrive as one `raw.share_point` table per file or sheet. `helplines_sources`
  finds them by name prefix at run time, so new monthly drops need no code change.
- One `partner_history_<partner>` model per partner unions, cleans and picks one version per
  month; `helplines_partner_history` unites them; `helplines_aggregated_full` is the mart.
  dbt tests replace the promote scripts' shape check.
- Version choice is "fullest month wins, ties to the earlier table name" (Fivetran carries no
  load order, so R's first-seen rule cannot be reproduced). Age UK takes the earliest-ending pair.
- The stable period needs 5 partners, as `data_discriminator.R` does. Trying 6 shrank the mart from 24 to 15 months, so it stays 5 until the State of the Nation data is combined (dbt-asc#147). The mart is `helplines_ongoing`, named so the State of the Nation output promoted alongside it stays distinguishable.
- History is retagged on every build, so a topic map change rewrites history. Snowflake never did.
- The LA lookup inputs (ONS LAD names, geography to LA, LA to CASSR) live in MotherDuck `reference.main` (`ons_lad_2025`, `geo_to_la`, `la_to_cassr`), not as dbt seeds; `helplines_la_lookup` derives from them.
- Not implemented (reparse branch only): the shared UT1 rule set, one source per partner per
  month with the 3% fallback stop, Carers UK and Age UK full series.
- Open: Scope and Age UK exports carry trailing spaces in labels. R does not trim, so those rows
  fail the map and leave the mart silently. Trim before the join or accept the loss.

External
- `stg_cld_*` are pass-throughs over `raw_external.main.cld_*`; `cld_raw_relation` is removed.
- Census and LANN tables map to the duckdb-only source `reference_external`; `ascfr_salt` is
  `ascfr_extract` on MotherDuck via a per-target `identifier`.
- Freshness on duckdb is `max(_extracted_at)` per source, replacing `snowflake_staleness_check.R`
  for external sources.

---

## Audit triggers

Re-read and update this charter when:
- A new raw database or schema is added as a dbt source
- `la_suppress` threshold changes (currently 5)
- A new mart model is added
- The pipeline cron schedule changes
- A source moves onto or off MotherDuck (update the disabled-on-duckdb list above)
- AdvicePro report `HS6T5CH9` or `FD7DXGL4` changes layout, or the count column stops collapsing rows
