"""Seed AVA.PUBLIC.CONTENTFUL_DATA (Snowflake, read-only) into MotherDuck.

Target: raw.contentful.contentful_data (full replace on each run).
Read by dbt as source('chatbot_raw', 'contentful_data') on the DuckDB target.

STOPGAP. Contentful must eventually land in MotherDuck natively (the producer is
chatbot_content's daily QC run, which still writes Snowflake). Until it does,
this script is the only thing keeping the DuckDB chatbot chain current, and
Snowflake cannot be retired. Re-run it whenever Contentful content changes.

Snowflake is SELECT-only here (ROLE_DEV, JWT key auth, same as ~/.dbt/profiles.yml).
Secrets are never printed. Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE.

Usage:  python scripts/seed/seed_contentful.py
"""
import os
import sys

import duckdb
import snowflake.connector
import yaml

SF_TABLE = ("AVA", "PUBLIC", "CONTENTFUL_DATA")
DUCK_DB, DUCK_SCHEMA, DUCK_TABLE = "raw", "contentful", "contentful_data"

TYPE_MAP = {
    "TEXT": "VARCHAR", "VARCHAR": "VARCHAR", "FLOAT": "DOUBLE", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP_NTZ": "TIMESTAMP", "TIMESTAMP_LTZ": "TIMESTAMPTZ",
    "TIMESTAMP_TZ": "TIMESTAMPTZ",
}


def sf_connect():
    prof = os.path.expanduser("~/.dbt/profiles.yml")
    with open(prof, encoding="utf-8-sig") as f:
        account = yaml.safe_load(f)["asc"]["outputs"]["prod"]["account"]
    return snowflake.connector.connect(
        account=account, user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_KEY_FILE"], authenticator="SNOWFLAKE_JWT",
        role="ROLE_DEV", warehouse="COMPUTE_WH", client_session_keep_alive=False,
        session_parameters={"QUERY_TAG": "seed_contentful_readonly"})


def duck_type(sf_type, scale):
    t = sf_type.upper()
    if t in TYPE_MAP:
        return TYPE_MAP[t]
    if t == "NUMBER":
        return "BIGINT" if not scale else "DOUBLE"
    sys.exit(f"unmapped Snowflake type {sf_type}")


def main():
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        sys.exit("MOTHERDUCK_TOKEN unset")
    db, sch, tab = SF_TABLE
    scon = sf_connect()
    cur = scon.cursor()
    cur.execute(
        f"SELECT column_name, data_type, numeric_scale FROM {db}.information_schema.columns "
        f"WHERE table_schema='{sch}' AND table_name='{tab}' ORDER BY ordinal_position")
    cols = [(n.lower(), duck_type(t, s)) for n, t, s in cur.fetchall()]
    if not cols:
        sys.exit("no columns found for the Snowflake source table")
    cur.execute(f"SELECT * FROM {db}.{sch}.{tab}")
    rows = cur.fetchall()
    scon.close()

    # Stage locally (fast executemany), then one bulk copy to MotherDuck.
    dcon = duckdb.connect()
    ddl = ", ".join(f'"{n}" {t}' for n, t in cols)
    dcon.execute(f"CREATE TABLE stage ({ddl})")
    ph = ", ".join("?" for _ in cols)
    dcon.executemany(f"INSERT INTO stage VALUES ({ph})", rows)
    dcon.execute(f"ATTACH 'md:{DUCK_DB}' AS md")
    dcon.execute(f"CREATE SCHEMA IF NOT EXISTS md.{DUCK_SCHEMA}")
    fq = f"md.{DUCK_SCHEMA}.{DUCK_TABLE}"
    dcon.execute(f"CREATE OR REPLACE TABLE {fq} AS SELECT * FROM stage")
    n = dcon.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    print(f"seeded {DUCK_DB}.{DUCK_SCHEMA}.{DUCK_TABLE}: {n} rows "
          f"(Snowflake source rows: {len(rows)}), {len(cols)} columns")
    if n != len(rows):
        sys.exit("row count mismatch after seed")


if __name__ == "__main__":
    main()
