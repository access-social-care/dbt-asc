# ILTA self-serve Power BI: build spec

Source models (dbt-asc, schema `PUBLIC_ILTA` in `ANALYTICS`; dbt prefixes the target schema):

| Table | Grain | Role in Power BI |
|---|---|---|
| `mart_ilta_demographics` | source x month x LA x org x feature x ILTA category | Fact |
| `dim_ilta_category` | feature x ILTA category (incl. status rows) | Dimension |
| (build in Power BI) `DimDate` | one row per month | Date table |

`record_count` = AccessAva conversations + AdvicePro cases. **Not unique clients.** Say so on every page footer.

## Model

- `mart_ilta_demographics[ILTA_KEY]` many-to-one `dim_ilta_category[ILTA_KEY]`, single direction.
- `mart_ilta_demographics[MONTH_DATE]` many-to-one `DimDate[Date]` (first of month).
- Hide `ILTA_KEY`, `FEATURE`, `ILTA_CATEGORY` on the fact; slice with the dimension columns. Sort `dim_ilta_category[ILTA_CATEGORY]` by `SORT_ORDER`.
- Slicers (fact columns): `DimDate` range or quarter, `SOURCE_SYSTEM`, `LA_NAME`, `ORG_NAME`.
- Import mode is fine (about 18k fact rows).

## Measures

```dax
Records = SUM ( mart_ilta_demographics[RECORD_COUNT] )

-- Count of a reported ILTA category (use the dimension on rows)
ILTA Count =
CALCULATE ( [Records], dim_ilta_category[IS_REPORTED] = TRUE () )

Not Recorded =
CALCULATE ( [Records], dim_ilta_category[ILTA_CATEGORY] = "Not recorded" )

Not Collected =
CALCULATE ( [Records], dim_ilta_category[ILTA_CATEGORY] = "Not collected" )

Unmapped =
CALCULATE ( [Records], dim_ilta_category[ILTA_CATEGORY] = "Unmapped" )

-- Records where this field could have been collected
Collectable = [Records] - [Not Collected]

Completeness % =
DIVIDE ( [Collectable] - [Not Recorded], [Collectable] )

-- "Do you consistently collect X?" suggested rule; agree the threshold with ILTA
Consistently Collected? =
IF ( [Completeness %] >= 0.9, "Yes", "No" )
```

`Records` ignores the `IS_REPORTED` filter on purpose; each FEATURE sums to the same total, so put only one FEATURE in context (page per feature) or the total is counted four times.

## Pages

1. **ILTA return.** One matrix per section (Gender, Age, Disability, Ethnicity): rows = `dim_ilta_category[ILTA_CATEGORY]` (reported only, via visual filter `IS_REPORTED = TRUE`), values = `[Records]`, plus a card for `[Completeness %]` and `[Consistently Collected?]`. Date slicer = the reporting period (previous return: 2025-10-01 to 2026-03-31).
2. **Explore.** Free-form: feature selector, category on rows, split by `SOURCE_SYSTEM` / `LA_NAME` / `ORG_NAME`, trend by month. This is the "come up with your own answers" page.
3. **Data quality.** `Completeness %` by feature x source x month; `Unmapped` count by feature; a note listing the map fixes below.

## Rules baked into the data (tell users)

- **AccessAva has no gender.** Rows are `Not collected`; they are excluded from gender completeness. Gender is AdvicePro only.
- **Disability** is derived: 'No' -> No disability; a stated type -> its ILTA label; 'Yes' with no type -> **Other**. ILTA has no "unspecified" bucket; confirm this is acceptable.
- **Age is lossy.** Source bands do not match ILTA bands: AccessAva `26-45` -> 25-34, `46-64` -> 45-54; AdvicePro `35 - 49` -> 35-44, `50 - 64` -> 55-64. AccessAva `65+` -> 65-74 (cannot give 75 Plus). Treat age counts as indicative.
- AccessAva coarse `AGE` (Under 18 / 18-64 / 65+, ~15% populated) is ignored; only `AGE_RANGE` (<1% populated) is used.
- `Unmapped` rows are shown, never dropped.

## Data volumes (dev, 2026-10-01)

AccessAva 26,992 conversations, under 2% with any demographic; AdvicePro 2,822 cases, about 5% with demographics. Expect "Consistently collected = No" for all four sections, and small numbers per category. Do not publish LA x month cuts externally: the mart is **not small-number suppressed** (counts of 1-2 occur). Grant the `PUBLIC_ILTA` schema only to the BI reader role, or add suppression (`la_suppress` pattern) before widening access.

## Known map defects (REFERENCE.PUBLIC, fix at source; not changed here)

| Table | Issue | Effect |
|---|---|---|
| `ILTA_MAP_AVA` | two dyslexia `disability_text` rows are column-shifted (ILTA = `e.g. dyslexia` / blank) | Resolved in `ilta_map_supplement` -> Cognitive disability (agreed 2026-10-01); source rows still need fixing |
| `ILTA_MAP_AVA` | `65+` -> 65-74 | 75 Plus never produced |
| `ILTA_MAP_CASEWORK` | `65+` missing | Resolved in `ilta_map_supplement` -> 65-74 (same as AccessAva; cannot give 75 Plus) |
| `ILTA_MAP_CASEWORK` | `Dementia` missing | currently hidden: the one case is 'No' disability, type 'Dementia' (source contradiction) |
| both | gender has no 'Other' value in either source | Other gender is always 0 |
| `ILTA_MAP_CASEWORK` | `35 - 49`, `50 - 64` straddle ILTA bands | lossy, see above |

Gap-fills that were unambiguous live in `seeds/ilta_map_supplement.csv` (25 - 34, White Irish, the 'Any other Black...' values; the AdvicePro one is truncated at 50 characters in the warehouse). Add rows there once the maps are agreed, or reload the reference tables and delete the rows.

The dev maps were read from `REFERENCE_DEV` (ROLE_DEV routing). Compare with prod `REFERENCE.PUBLIC` before the production run.

## To publish

1. PR for `feat/ilta-demographics-mart`; on merge run `dbt build --select +mart_ilta_demographics +dim_ilta_category` on prod (includes the two seeds).
2. Create/confirm a BI reader role with USAGE on `ANALYTICS.PUBLIC_ILTA` and SELECT on its tables (not granted by this change).
3. Build the report in Power BI Desktop to the model above.
