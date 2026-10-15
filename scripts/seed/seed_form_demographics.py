"""Seed the external equalities-form demographics into MotherDuck.

Target: analytics_dev.landing.chatbot_form_demographics (full replace on each run).
Read by dbt as source('chatbot_landing', 'chatbot_form_demographics') on the DuckDB
target; the accessava model coalesces it into ethnicity, age_range, person_type,
disability_n_y and armed_service exactly as ascFuncs::process_raw_data does.

Input is ascFuncs inst/extdata/demographics_matched.RDS (about 170 form responses
matched to transcripts). It is personal-data adjacent: this script never prints
values and writes nothing into the repo. R (Rscript) exports the six columns the
join uses to a CSV in a private temp directory, which is deleted after loading.
Only transcript_id plus those five columns are carried; the RDS holds other form
fields (religion, english_difficulty, disability_text, form timing) that
process_raw_data does not use, so they are not copied.

When the producer is retired, the form should land in MotherDuck natively and this
script goes away.

Env: MOTHERDUCK_TOKEN. Usage:
  python scripts/seed/seed_form_demographics.py [--rds PATH] [--rscript PATH]
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile

import duckdb

DEFAULT_RDS = r"C:\o\_ASC\ascFuncs\inst\extdata\demographics_matched.RDS"
DEFAULT_RSCRIPT = r"C:\Program Files\R\R-4.5.1\bin\Rscript.exe"
COLS = ["transcript_id", "ethnicity", "age_range", "person_type", "disability_n_y", "armed_service"]
DUCK_DB, DUCK_SCHEMA, DUCK_TABLE = "analytics_dev", "landing", "chatbot_form_demographics"

R_EXPORT = (
    "args <- commandArgs(trailingOnly = TRUE); "
    "x <- readRDS(args[1]); "
    "x <- x[, c('transcript_id','ethnicity','age_range','person_type','disability_n_y','armed_service')]; "
    "utils::write.csv(x, args[2], row.names = FALSE, na = '')"
)


def main():
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        sys.exit("MOTHERDUCK_TOKEN is not set")
    ap = argparse.ArgumentParser()
    ap.add_argument("--rds", default=DEFAULT_RDS)
    ap.add_argument("--rscript", default=shutil.which("Rscript") or DEFAULT_RSCRIPT)
    a = ap.parse_args()
    if not os.path.exists(a.rds):
        sys.exit("RDS file not found")

    tmp = tempfile.mkdtemp(prefix="form_demog_")
    try:
        csv_path = os.path.join(tmp, "form_demographics.csv")
        subprocess.run([a.rscript, "-e", R_EXPORT, a.rds, csv_path], check=True,
                       stdout=subprocess.DEVNULL)
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE stage AS SELECT " + ", ".join(
                f"NULLIF({c}, '') AS {c}" for c in COLS)
            + f" FROM read_csv('{csv_path}', header=true, all_varchar=true)")
        n, nd = con.execute("SELECT COUNT(*), COUNT(DISTINCT transcript_id) FROM stage").fetchone()
        if n != nd:
            sys.exit(f"duplicate transcript_id in form data ({n} rows, {nd} distinct); "
                     "process_raw_data would duplicate rows - resolve before loading")
        con.execute(f"ATTACH 'md:{DUCK_DB}' AS md")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS md.{DUCK_SCHEMA}")
        con.execute(f"CREATE OR REPLACE TABLE md.{DUCK_SCHEMA}.{DUCK_TABLE} AS SELECT * FROM stage")
        print(f"seeded {DUCK_DB}.{DUCK_SCHEMA}.{DUCK_TABLE}: {n} rows, {len(COLS)} columns")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
