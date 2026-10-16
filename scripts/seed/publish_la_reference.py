"""Publish the three local-authority lookup tables to MotherDuck `reference.main`.

Source files: scripts/seed/reference_la/*.csv (lifted from the ascFuncs package data used by
local_authority_fixer() and cassr_converter()). Creates a table only when it does not exist;
never replaces or touches an existing reference table. Regenerate the CSVs from ascFuncs and
delete the MotherDuck table by hand if the content must change.
"""
import os
import sys

import duckdb

TABLES = ["ons_lad_2025", "geo_to_la", "la_to_cassr"]
HERE = os.path.dirname(os.path.abspath(__file__))

if not os.environ.get("MOTHERDUCK_TOKEN"):
    raise SystemExit("MOTHERDUCK_TOKEN not set")

con = duckdb.connect("md:reference")
existing = {r[0] for r in con.execute(
    "select table_name from duckdb_tables() where database_name = 'reference' and schema_name = 'main'"
).fetchall()}
for name in TABLES:
    if name in existing:
        print(f"skip {name}: already in reference.main")
        continue
    csv = os.path.join(HERE, "reference_la", f"{name}.csv")
    rel = con.read_csv(csv, all_varchar=True)
    rel.to_table(f"main.{name}")
    n = con.execute(f"select count(*) from main.{name}").fetchone()[0]
    print(f"published reference.main.{name}: {n} rows")
sys.exit(0)
