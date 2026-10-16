"""Land AVA.PUBLIC.CONTENTFUL_DATA (Snowflake, read-only) in MotherDuck raw.contentful.contentful_data.

Full overwrite on every run. Column names lower-case, types as Snowflake
(TEXT -> VARCHAR, FLOAT -> DOUBLE, BOOLEAN -> BOOLEAN). Read by dbt through the
source raw.contentful and the staging model stg_contentful_data.

STOPGAP. The producer is chatbot_content's daily QC job
(content_quality_assurance.Rmd), which still writes Snowflake only. Until that
job writes raw.contentful itself, run this script after it. Snowflake cannot be
retired until then.

Snowflake is SELECT-only (ROLE_DEV, JWT key auth, same as ~/.dbt/profiles.yml).
Secrets are never printed; only the row count is logged.
Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE.

Usage:  python scripts/seed/land_contentful_raw.py
"""
import json
import os
import sys
import tempfile

if not os.environ.get("MOTHERDUCK_TOKEN"):
    raise SystemExit("MOTHERDUCK_TOKEN not set")

import duckdb
import snowflake.connector
import yaml

SF_DB, SF_SCHEMA, SF_TABLE = "AVA", "PUBLIC", "CONTENTFUL_DATA"
MD_DB, MD_SCHEMA, MD_TABLE = "raw", "contentful", "contentful_data"
TYPE_MAP = {"TEXT": "VARCHAR", "VARCHAR": "VARCHAR", "FLOAT": "DOUBLE", "BOOLEAN": "BOOLEAN"}


def sf_connect():
    prof = os.path.expanduser("~/.dbt/profiles.yml")
    with open(prof, encoding="utf-8-sig") as f:
        account = yaml.safe_load(f)["asc"]["outputs"]["prod"]["account"]
    return snowflake.connector.connect(
        account=account, user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_KEY_FILE"], authenticator="SNOWFLAKE_JWT",
        role="ROLE_DEV", warehouse="COMPUTE_WH", client_session_keep_alive=False,
        session_parameters={"QUERY_TAG": "land_contentful_raw_readonly"})


def main():
    scon = sf_connect()
    cur = scon.cursor()
    cur.execute(
        f"SELECT column_name, data_type FROM {SF_DB}.information_schema.columns "
        f"WHERE table_schema='{SF_SCHEMA}' AND table_name='{SF_TABLE}' ORDER BY ordinal_position")
    cols = []
    for name, sf_type in cur.fetchall():
        if sf_type.upper() not in TYPE_MAP:
            sys.exit(f"unmapped Snowflake type {sf_type} for {name}")
        cols.append((name.lower(), TYPE_MAP[sf_type.upper()]))
    if not cols:
        sys.exit("no columns found for the Snowflake source table")
    cur.execute(f"SELECT * FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}")
    rows = cur.fetchall()
    scon.close()

    # Stage as JSON lines (types survive: str, float, bool, null), then one
    # relational-API write to MotherDuck.
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "contentful.jsonl")
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            for r in rows:
                f.write(json.dumps({c[0]: v for c, v in zip(cols, r)}) + "\n")
        con = duckdb.connect(f"md:{MD_DB}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA}")
        rel = con.read_json(path, format="newline_delimited", columns=dict(cols))
        con.execute(f"DROP TABLE IF EXISTS {MD_SCHEMA}.{MD_TABLE}")
        rel.to_table(f"{MD_SCHEMA}.{MD_TABLE}")
        n = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    print(f"landed {MD_DB}.{MD_SCHEMA}.{MD_TABLE}: {n} rows (Snowflake rows: {len(rows)})")
    if n != len(rows):
        sys.exit("row count mismatch after landing")


if __name__ == "__main__":
    main()
