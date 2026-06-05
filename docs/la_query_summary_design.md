## New cross-source LA query summary mart

Three models added in `feat/la-query-summary-mart`:

- `stg_accessava_segments` — AccessAva conversations with `CASE_SPECIFIC_ISSUES_GROUP` flattened on `;` via `LATERAL FLATTEN`. One row per conversation × segment. Grain enables segment-level counts.
- `stg_helplines` — `HELPLINES_AGGREGATED_FULL` staged to the shared column interface. `SEGMENT = UT1`, `QUERY_COUNT = N` (pre-aggregated).
- `mart_la_query_summary` — `UNION ALL` of AdvicePro + AccessAva segments + Helplines, grouped to `LA_NAME × SOURCE_SYSTEM × SEGMENT`. This is the baseline query count table for all comparison analysis.

### Segment source mapping

| SOURCE_SYSTEM | SEGMENT field | Taxonomy |
|---|---|---|
| `AdvicePro` | `NULL` | No shared taxonomy yet |
| `AccessAva` | `CASE_SPECIFIC_ISSUES_GROUP` (flattened) | AccessAva categories |
| `Helplines` | `UT1` | 9 values: Assessments, Care plan, Carers, Charging, Direct payments, Information Seeking, Legal issues and complaints, Mental capacity, Safeguarding |

### Still to do

- [ ] Set up LA peer groups model (venv in `ASC_LA_Peer_Groups`, configure `config.toml`, run, write `REFERENCE.LA_PEER_GROUPS`)
- [ ] Create `asc-reference-data` catalog repo with `DATASET.md` files for IMD, ASCFR, peer groups output
- [ ] Quadrant analysis (deprivation × hits/capita, spend × hits/capita) — R Rmd, reads local files + Snowflake mart
- [ ] Grant `ROLE_DEV` SELECT on `ACCESSAVA` so `stg_accessava_segments` can be tested locally
