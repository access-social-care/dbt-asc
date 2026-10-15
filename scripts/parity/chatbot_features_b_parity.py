"""Parity for chatbot feature models, group B (int_chatbot_* in models/staging/chatbot/features_b).

Oracle: Snowflake AVA.PUBLIC.ACCESSAVA (the R pipeline's final joined output), SELECT-only.
Per ACCESSAVA column, over the conversations in ACCESSAVA at a frozen cutoff, compares the
value with the DuckDB model value (NULL when the model has no row for that transcript).
Only feature columns are pulled (no tenant, no demographics). Nothing is written to either side.

Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE (never printed).

  python scripts/parity/chatbot_features_b_parity.py --cutoff 2026-10-13 --out docs/parity/chatbot_features_b.md
  add --examples N --debug-dir DIR to dump N mismatching transcript_id/value pairs per column to DIR (local only).
"""
import argparse
import csv
import os
import sys
import tempfile
from datetime import date

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parity  # noqa: E402

EV = "md.staging.stg_chatbot_transcript_event"
TR = "md.staging.stg_chatbot_transcript"

# (model, [(accessava column, model column, kind)]) ; kind 'text' or 'bool'
FEATURES = [
    ("int_chatbot_persona", [("persona", "persona", "text")]),
    ("int_chatbot_s_c_csi", [("supercategories", "supercategories", "text"), ("categories", "categories", "text"),
                              ("csis", "csis", "text")]),
    ("int_chatbot_letters_downloaded", [("lettercode", "lettercode", "text"),
                                         ("letter_downloaded", "letter_downloaded", "bool")]),
    ("int_chatbot_autocomplete_starts", [("autocomplete_start", "autocomplete_start", "bool")]),
    ("int_chatbot_autocomplete_finish", [("autocomplete_finish", "autocomplete_finish", "bool")]),
    ("int_chatbot_factsheets_templates", [("factsheets_templates", "factsheets_templates", "bool")]),
    ("int_chatbot_rasa", [("rasa", "rasa", "bool")]),
    ("int_chatbot_topic_entry_point", [("topic_entry_point", "topic_entry_point", "text")]),
    ("int_chatbot_consumed_resources", [("consumed_resources", "consumed_resources", "text")]),
    ("int_chatbot_age_of_person", [("age", "age", "text")]),
    ("int_chatbot_conversation_source", [("referring_page_url", "referring_page_url", "text"),
                                          ("referring_page_parameters", "referring_page_parameters", "text")]),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", required=True)
    ap.add_argument("--out")
    ap.add_argument("--examples", type=int, default=0)
    ap.add_argument("--debug-dir")
    a = ap.parse_args()

    cols = [c for _, cs in FEATURES for c, _, _ in cs]
    c = duckdb.connect()
    c.execute("ATTACH 'md:analytics_dev' AS md")

    scon = parity.sf_connect()
    cur = scon.cursor()
    sql = "SELECT transcript_id, created_at, " + ", ".join(cols) + " FROM ava.public.accessava"
    parity.guard(sql)
    cur.execute(sql)
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["transcript_id", "created_at"] + cols)
        w.writerows([["\\N" if v is None else v for v in row] for row in cur.fetchall()])
        sf_csv = f.name
    scon.close()
    types = {"transcript_id": "VARCHAR", "created_at": "DATE"}
    for _, cs in FEATURES:
        for col, _, kind in cs:
            types[col] = "BOOLEAN" if kind == "bool" else "VARCHAR"
    c.execute(f"CREATE TABLE sf AS SELECT * FROM read_csv('{sf_csv}', header=true, types={types!r}, "
              f"nullstr=['\\N'], quote='\"') WHERE created_at <= DATE '{a.cutoff}'")
    os.unlink(sf_csv)

    # id universe on the dbt side (same rule as chatbot_base_parity.py)
    c.execute(f"""CREATE TABLE base_u AS
        SELECT transcript_id FROM (
          SELECT transcript_id FROM {EV} WHERE entry_day <= DATE '{a.cutoff}' AND is_reporting_tenant
          UNION
          SELECT id FROM {TR} WHERE entry_day <= DATE '{a.cutoff}' AND is_reporting_tenant AND is_engaged)""")

    n_sf = c.execute("SELECT COUNT(*) FROM sf").fetchone()[0]
    md = ["# Parity: chatbot feature models, group B", "",
          f"- Date: {date.today().isoformat()}  |  frozen cutoff: **{a.cutoff}**  |  oracle: AVA.PUBLIC.ACCESSAVA ({n_sf} conversations)",
          "- Per column, over ACCESSAVA conversations: model value (NULL when the model has no row) vs ACCESSAVA value.",
          "- 'dbt-only ids' = feature rows for conversations in the dbt id universe but not in ACCESSAVA at the cutoff.", "",
          "| model.column | ACCESSAVA non-null | dbt non-null | equal | differ | ACCESSAVA only | dbt only | dbt-only ids |",
          "|---|---|---|---|---|---|---|---|"]
    for model, cs in FEATURES:
        c.execute(f"CREATE OR REPLACE TABLE m AS SELECT * FROM md.staging.{model}")
        dup = c.execute("SELECT COUNT(*) - COUNT(DISTINCT transcript_id) FROM m").fetchone()[0]
        extra = c.execute("""SELECT COUNT(*) FROM m WHERE transcript_id IN (SELECT transcript_id FROM base_u)
                             AND transcript_id NOT IN (SELECT transcript_id FROM sf)""").fetchone()[0]
        for sfcol, mcol, kind in cs:
            r = c.execute(f"""
                SELECT COUNT(*) FILTER (WHERE s.{sfcol} IS NOT NULL),
                       COUNT(*) FILTER (WHERE m.{mcol} IS NOT NULL),
                       COUNT(*) FILTER (WHERE s.{sfcol} IS NOT NULL AND m.{mcol} IS NOT NULL AND s.{sfcol} = m.{mcol}),
                       COUNT(*) FILTER (WHERE s.{sfcol} IS NOT NULL AND m.{mcol} IS NOT NULL AND s.{sfcol} <> m.{mcol}),
                       COUNT(*) FILTER (WHERE s.{sfcol} IS NOT NULL AND m.{mcol} IS NULL),
                       COUNT(*) FILTER (WHERE s.{sfcol} IS NULL AND m.{mcol} IS NOT NULL)
                FROM sf s LEFT JOIN m ON s.transcript_id = m.transcript_id""").fetchone()
            md.append(f"| {model}.{mcol} | " + " | ".join(str(x) for x in r) + f" | {extra} |"
                      + (f" (duplicate transcript_id rows: {dup})" if dup else ""))
            if a.examples and a.debug_dir:
                os.makedirs(a.debug_dir, exist_ok=True)
                rows = c.execute(f"""
                    SELECT s.transcript_id, s.{sfcol}::VARCHAR, m.{mcol}::VARCHAR FROM sf s
                    LEFT JOIN m ON s.transcript_id = m.transcript_id
                    WHERE s.{sfcol} IS DISTINCT FROM m.{mcol} LIMIT {a.examples}""").fetchall()
                with open(os.path.join(a.debug_dir, f"{model}.{mcol}.tsv"), "w", encoding="utf-8") as f:
                    for row in rows:
                        f.write("\t".join("" if v is None else str(v) for v in row) + "\n")
    text = "\n".join(md) + "\n"
    print(text)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)


if __name__ == "__main__":
    main()
