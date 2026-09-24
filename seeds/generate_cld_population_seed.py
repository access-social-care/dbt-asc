"""Regenerate seeds/cld_published_population.csv.

A ONE-OFF generator, not part of any scheduled pipeline. Run it by hand only
when a source below actually changes - which, for both of them, is rare by
construction.

Run from the repo root:
    python seeds/generate_cld_population_seed.py

TWO DENOMINATOR BASES
---------------------
The seed carries one row per (area_code, dimension, dimension_value,
population_base). The two bases answer different questions and are NOT
interchangeable:

  published      Whole population of that group, as published by DHSC in
                 Tables 4-6 of the CLD quarterly ODS files. A rate on this
                 basis is "per head of everyone".

  eligible_population
                 People who could plausibly need adult social care: everyone
                 aged 65+, plus working-age people recorded as disabled under
                 the Equality Act. From the ONS Census 2021 RM070 custom
                 cross-tab. A rate on this basis is an ACCESS rate - "of the
                 people who might need care, what share are getting it" -
                 rather than a utilisation share.

WHY THE PUBLISHED FIGURES ARE A SEED AND NOT A LANDING TABLE
------------------------------------------------------------
The Population column is byte-identical across the 2025-09, 2025-12 and
2026-03 releases (England 46,437,085 in all three, verified 2026-09-22). It
is a frozen constant, so re-fetching it every quarter would imply a freshness
it does not have.

COVERAGE IS DELIBERATELY ASYMMETRIC
-----------------------------------
The eligible_population basis covers far less than the published one,
because RM070 is a coarser cross-tab:

                   published             eligible_population
  age_group        9 values             3 ("All", "18 to 64", "65 and above")
  gender           3 values             none (RM070 has no sex dimension)
  ethnicity        25 values            6 (the top-level groups + "All")
  area_unit        LA, Region, National LA + National only

eligible_population coverage is a strict SUBSET of published coverage on
every key, which is what lets the NORMALISED models inner-join the published basis and
left-join this one on top. tests/assert_cld_population_basis_subset.sql
enforces that, so if a future regeneration breaks the subset property it
fails loudly instead of silently dropping rows from NORMALISED.

KNOWN IMPRECISION, STATED RATHER THAN HIDDEN
--------------------------------------------
1. RM070's working-age band is 16-64. CLD's is 18-64, and the CLD Notes sheet
   confirms under-18s are excluded from the published statistics. So the
   eligible_population denominator for "18 to 64" carries a 16-17 overhang
   and slightly UNDERSTATES the rate. RM070 has no finer age split available, so
   this cannot be fixed from this source. (The same approximation already
   exists, unflagged, in signal_processing/external/03_segments.R line 19.)

2. Census 2021 is a March 2021 snapshot; the CLD data runs to 2026. Neither
   basis is contemporaneous with the numerator, but the published basis at
   least uses a mid-year estimate for age and gender.

3. DISCREPANCY WITH signal_processing/external/03_segments.R - decide before
   relying on cross-comparisons. That script uses TWO different definitions
   of "eligible" in the same file:
     - overall (line 18):   ALL 65+  +  disabled 16-64
     - per ethnicity (code): disabled 65+  +  disabled 16-64
   so its ethnicity denominators are systematically smaller than its overall
   one, making ethnicity rates systematically higher and not comparable with
   the overall rate. This script uses the line-18 definition (ALL 65+) for
   every dimension, consistently. If the 03_segments.R behaviour turns out to
   be deliberate rather than a slip, this choice needs revisiting - it was
   not obvious from the code which was intended.

SOURCES
-------
published:
    The three CLD quarterly ODS files. Any vintage works - the Population
    column is identical in all of them. Tables 4, 5 and 6 of each of the
    assessments and long-term-support files.
    https://www.gov.uk/government/collections/adult-social-care-client-level-data-england-quarterly-update

eligible_population:
    _ASC/signal_processing/lann/census2021-rm070-custom/
        RM070-disability-age-ethnicity-utla-2021.xlsx
    ONS Census 2021 RM070, custom cross-tab (disability x age x ethnicity x
    UTLA) built via the ONS flexible table builder - it is not a standard
    bulk download. See that folder's README.md to reproduce it.
"""

from __future__ import annotations

import csv
import io
import os
import re
import sys
import zipfile

import pandas as pd

# --------------------------------------------------------------------------
# Paths. The ODS files are not vendored into this repo - point at wherever
# they were downloaded. The RM070 workbook lives in signal_processing.
# --------------------------------------------------------------------------
ODS_DIR = os.environ.get("CLD_ODS_DIR", ".")
ASSESS_ODS = os.path.join(ODS_DIR, "assessments.ods")
LTS_ODS = os.path.join(ODS_DIR, "long_term_support.ods")

RM070_XLSX = os.environ.get(
    "RM070_XLSX",
    os.path.join(
        "..", "signal_processing", "lann", "census2021-rm070-custom",
        "RM070-disability-age-ethnicity-utla-2021.xlsx",
    ),
)

OUT_PATH = os.path.join("seeds", "cld_published_population.csv")

ENGLAND_CODE = "E92000001"

# RM070's ethnic-group labels carry Welsh variants (the cross-tab covers
# England and Wales); CLD's do not. Mapped explicitly rather than fuzzily so
# a relabelled category fails loudly on the assertion below.
RM070_ETHNICITY_TO_CLD = {
    "White": "White",
    "Asian, Asian British or Asian Welsh": "Asian or Asian British",
    "Black, Black British, Black Welsh, Caribbean or African":
        "Black, Black British, Caribbean or African",
    "Mixed or Multiple ethnic groups": "Mixed or multiple ethnic groups",
    "Other ethnic group": "Other ethnic group",
    # "Does not apply" is a structural artifact with a count of exactly 0 in
    # every cell (verified 2026-09-22), so dropping it loses nothing and the
    # five real groups still sum to the full population.
}

DISABLED_PREFIX = "Disabled under the Equality Act"
AGE_WORKING = "Aged 16 to 64 years"
AGE_OLDER = "Aged 65 years and over"


# --------------------------------------------------------------------------
# published basis - from the CLD ODS files
# --------------------------------------------------------------------------
def _ods_sheet_rows(path: str, sheet: str) -> list[list[str]]:
    """Read one ODS sheet as rows of strings, PRESERVING EMPTY CELLS.

    Positional integrity is load-bearing here: England and Region rows have
    an empty "LA code" cell, so compressing empties out of a row shifts every
    column after it and silently drops exactly the national and regional rows
    this seed needs.
    """
    with zipfile.ZipFile(path) as z:
        xml = z.read("content.xml").decode()
    m = re.search(r'<table:table table:name="%s"[^>]*>' % sheet, xml)
    if m is None:
        raise ValueError(f"{path}: no sheet named {sheet}")
    seg = xml[m.end(): xml.index("</table:table>", m.end())]

    rows = []
    for rm in re.finditer(
        r"<table:table-row[^>]*>(.*?)</table:table-row>", seg, re.S
    ):
        cells: list[str] = []
        for cm in re.finditer(
            r"<table:table-cell([^>]*)/?>(.*?)?(?=<table:table-cell|$)",
            rm.group(1),
            re.S,
        ):
            attrs, body = cm.group(1), cm.group(2) or ""
            repeat = re.search(r'number-columns-repeated="(\d+)"', attrs)
            text = re.sub(
                r"<[^>]+>",
                "",
                "".join(re.findall(r"<text:p>(.*?)</text:p>", body, re.S)),
            )
            n = int(repeat.group(1)) if repeat else 1
            # A large repeat count is the trailing run of empty cells that
            # pads a row out to the sheet width, not real columns.
            cells += [text] * (1 if n > 50 else n)
        rows.append(cells)
    return rows


def _as_count(raw: str) -> str:
    raw = (raw or "").replace(",", "").strip()
    return raw if re.fullmatch(r"\d+", raw) else ""


def build_published() -> dict[tuple[str, str, str], tuple[str, str, str]]:
    dim_header = {
        "age_group": "Age group",
        "gender": "Gender",
        "ethnicity": "Ethnicity",
    }
    specs = [
        (ASSESS_ODS, "Table_4", "age_group"),
        (ASSESS_ODS, "Table_5", "gender"),
        (ASSESS_ODS, "Table_6", "ethnicity"),
        (LTS_ODS, "Table_4", "age_group"),
        (LTS_ODS, "Table_5", "gender"),
        (LTS_ODS, "Table_6", "ethnicity"),
    ]

    seen: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    conflicts = []

    for path, sheet, dim in specs:
        rows = _ods_sheet_rows(path, sheet)
        header_i, header = next(
            (i, r) for i, r in enumerate(rows) if "Area" in r
        )
        i_area = header.index("Area")
        i_dim = header.index(dim_header[dim])
        i_code = header.index("Area code")
        i_unit = header.index("Area unit")
        i_pop = header.index("Population")

        for row in rows[header_i + 1:]:
            if len(row) <= max(i_dim, i_code, i_unit, i_pop):
                continue
            code = row[i_code].strip()
            pop = _as_count(row[i_pop])
            if not pop or not code.startswith("E"):
                continue
            key = (code, dim, row[i_dim].strip())
            rec = (row[i_area].strip(), row[i_unit].strip(), pop)
            if key in seen and seen[key][2] != pop:
                conflicts.append((key, seen[key][2], pop))
            seen.setdefault(key, rec)

    # The two files publish the same denominators. If they ever disagree, the
    # "population is a function of (area, dimension, value) alone" assumption
    # this seed rests on is wrong and needs revisiting, not averaging.
    if conflicts:
        raise SystemExit(
            f"published basis: {len(conflicts)} disagreements between the "
            f"assessments and long-term-support files, e.g. {conflicts[:3]}"
        )
    return seen


# --------------------------------------------------------------------------
# eligible_population basis - from Census 2021 RM070
# --------------------------------------------------------------------------
def build_eligible_population(
    la_codes: set[str], la_names: dict[str, str]
) -> dict[tuple[str, str, str], tuple[str, str, str]]:
    df = pd.read_excel(RM070_XLSX, sheet_name="Dataset")
    df["Observation"] = pd.to_numeric(df["Observation"], errors="coerce")

    col_area = "2023 Upper tier local authorities Code"
    col_dis = "Disability (5 categories)"
    col_age = "Age (3 categories)"
    col_eth = "Ethnic group (6 categories)"

    unmapped = set(df[col_eth].dropna().unique()) - set(
        RM070_ETHNICITY_TO_CLD
    ) - {"Does not apply"}
    if unmapped:
        raise SystemExit(
            f"RM070 ethnic-group labels not in the mapping: {sorted(unmapped)}"
        )

    df = df[df[col_area].isin(la_codes)].copy()
    df["eth"] = df[col_eth].map(RM070_ETHNICITY_TO_CLD)

    disabled = df[col_dis].str.startswith(DISABLED_PREFIX, na=False)

    # eligible = everyone 65+, PLUS working-age people recorded disabled.
    # Age alone is the qualifying criterion for the older band - that is the
    # definition used for the overall denominator in 03_segments.R line 18,
    # and it is applied here to EVERY dimension so the bases stay comparable
    # (see the module docstring, note 3).
    working_disabled = df[disabled & (df[col_age] == AGE_WORKING)]
    older_all = df[df[col_age] == AGE_OLDER]

    out: dict[tuple[str, str, str], tuple[str, str, str]] = {}

    def put(code: str, dim: str, value: str, pop: float) -> None:
        pop = int(round(pop))
        if pop <= 0:
            return
        out[(code, dim, value)] = (
            la_names.get(code, ""),
            "National" if code == ENGLAND_CODE else "Local Authority",
            str(pop),
        )

    # --- per ethnicity -----------------------------------------------------
    eth_work = working_disabled.groupby([col_area, "eth"]).Observation.sum()
    eth_old = older_all.groupby([col_area, "eth"]).Observation.sum()
    eth_total = eth_work.add(eth_old, fill_value=0)

    for (code, eth), pop in eth_total.items():
        if pd.isna(eth):
            continue
        put(code, "ethnicity", eth, pop)

    for code, pop in eth_total.groupby(level=0).sum().items():
        put(code, "ethnicity", "All", pop)

    # --- per age band ------------------------------------------------------
    work_by_la = working_disabled.groupby(col_area).Observation.sum()
    old_by_la = older_all.groupby(col_area).Observation.sum()

    for code, pop in work_by_la.items():
        put(code, "age_group", "18 to 64", pop)
    for code, pop in old_by_la.items():
        put(code, "age_group", "65 and above", pop)
    for code, pop in work_by_la.add(old_by_la, fill_value=0).items():
        put(code, "age_group", "All", pop)

    # --- England, by summing its constituent LAs ---------------------------
    # Regions are NOT produced: this needs a region-to-LA membership list,
    # which neither RM070 nor the CLD files provide. Those rows simply have
    # no eligible_population basis, which the subset test tolerates.
    eng_eth = eth_total.groupby(level=1).sum()
    for eth, pop in eng_eth.items():
        if pd.isna(eth):
            continue
        put(ENGLAND_CODE, "ethnicity", eth, pop)
    put(ENGLAND_CODE, "ethnicity", "All", eng_eth.sum())

    put(ENGLAND_CODE, "age_group", "18 to 64", work_by_la.sum())
    put(ENGLAND_CODE, "age_group", "65 and above", old_by_la.sum())
    put(
        ENGLAND_CODE,
        "age_group",
        "All",
        work_by_la.sum() + old_by_la.sum(),
    )

    # gender: RM070 carries no sex dimension, so there is nothing to emit.
    return out


# --------------------------------------------------------------------------
def main() -> None:
    published = build_published()

    la_codes = {
        code for (code, _, _), (_, unit, _) in published.items()
        if unit == "Local Authority"
    }
    la_names = {
        code: name for (code, _, _), (name, _, _) in published.items()
    }
    print(f"published: {len(published)} rows, {len(la_codes)} LAs")

    asc = build_eligible_population(la_codes, la_names)
    print(f"eligible_population: {len(asc)} rows")

    # Subset property, asserted here as well as in dbt: every eligible_population key
    # must also exist on the published basis. The NORMALISED models rely on
    # this - they inner-join published and left-join eligible_population, so an
    # eligible_population-only key would simply never appear.
    orphans = sorted(set(asc) - set(published))
    if orphans:
        raise SystemExit(
            f"{len(orphans)} eligible_population keys have no published "
            f"counterpart, e.g. {orphans[:5]}"
        )

    rows = []
    for basis, data in (("published", published), ("eligible_population", asc)):
        for (code, dim, value), (name, unit, pop) in data.items():
            rows.append([code, name, unit, dim, value, basis, pop])
    rows.sort(key=lambda r: (r[0], r[3], r[4], r[5]))

    with io.open(OUT_PATH, "w", encoding="utf-8", newline="\n") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow([
            "area_code", "area_name", "area_unit",
            "dimension", "dimension_value", "population_base", "population",
        ])
        w.writerows(rows)

    print(f"wrote {len(rows)} rows -> {OUT_PATH}")


if __name__ == "__main__":
    sys.exit(main())
