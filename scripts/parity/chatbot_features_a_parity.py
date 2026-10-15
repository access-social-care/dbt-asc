"""Parity for the chatbot feature models, group A (int_chatbot_*), against Snowflake
AVA.PUBLIC.ACCESSAVA, the R pipeline's final output.

ACCESSAVA is the full join of every chatbot_* function, so each model is compared
column by column over ALL ACCESSAVA transcript_ids at the cutoff: a transcript the
model has no row for counts as NULL. Per column the report gives, on ACCESSAVA ids:
  both NULL | equal | only ACCESSAVA has a value | only dbt has a value | both, different
The model-side set is not restricted, so every extra non-NULL dbt value on an
ACCESSAVA id shows up as "only dbt".

Snowflake is SELECT-only (ROLE_DEV); only the columns listed in SF_COLS are pulled
(one pass, to a local file that is deleted), nothing is written to Snowflake or
MotherDuck. Free-text columns (churn_point, user_feedback) are never printed.

Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE (never printed).

Example:
  python scripts/parity/chatbot_features_a_parity.py --cutoff 2026-10-13 ^
      --out docs/parity/chatbot_features_a.md
"""
import argparse
import csv
import os
import sys
import tempfile
from datetime import date

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parity  # noqa: E402  (reuses sf_connect + SELECT-only guard)

MD = "md:analytics_dev"
SCHEMA = "md.staging"
SF_COLS = ["transcript_id", "created_at", "tenant_name", "engaged", "region", "organisation_type",
           "completed", "ethnicity", "age_range", "person_type", "disability_n_y", "disability_text",
           "armed_service", "la_name", "dif", "churn_point", "churn_category", "rating", "user_feedback"]
FREE_TEXT = {"churn_point", "user_feedback"}

# model -> (function, ACCESSAVA columns it contributes, numeric columns, boolean columns)
FEATURES = [
    ("int_chatbot_total_convos", "chatbot_total_convos", ["created_at", "tenant_name", "engaged", "region", "organisation_type"], {"engaged"}, set()),
    ("int_chatbot_completions", "chatbot_completions", ["completed"], set(), {"completed"}),
    ("int_chatbot_ethnicity", "chatbot_ethnicity", ["ethnicity"], set(), set()),
    ("int_chatbot_age_group", "chatbot_age_group", ["age_range"], set(), set()),
    ("int_chatbot_person_type", "chatbot_person_type", ["person_type"], set(), set()),
    ("int_chatbot_disability", "chatbot_disability", ["disability_n_y", "disability_text"], set(), set()),
    ("int_chatbot_armed_forces", "chatbot_armed_forces", ["armed_service"], set(), set()),
    ("int_chatbot_la", "chatbot_la", ["la_name"], set(), set()),
    ("int_chatbot_time_spent", "chatbot_time_spent", ["dif"], {"dif"}, set()),
    ("int_chatbot_churn", "chatbot_churn", ["churn_point", "churn_category"], set(), set()),
    ("int_chatbot_ratings", "chatbot_ratings", ["rating"], {"rating"}, set()),
    ("int_chatbot_feedback", "chatbot_feedback", ["user_feedback"], set(), set()),
]


def pull_accessava(c, cutoff, cache):
    if cache and os.path.exists(cache):
        c.execute(f"CREATE TABLE sf AS SELECT * FROM read_parquet('{cache}')")
        return
    scon = parity.sf_connect()
    cur = scon.cursor()
    sql = f"SELECT {', '.join(SF_COLS)} FROM ava.public.accessava"
    parity.guard(sql)
    cur.execute(sql)
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(SF_COLS)
        while True:
            rows = cur.fetchmany(20000)
            if not rows:
                break
            w.writerows(rows)
        path = f.name
    scon.close()
    c.execute(f"CREATE TABLE sf AS SELECT * FROM read_csv('{path}', header=true, all_varchar=true, nullstr='')")
    os.unlink(path)
    if cache:
        c.execute(f"COPY sf TO '{cache}' (FORMAT parquet)")


def cmp_expr(col, numeric, boolean):
    if numeric:
        s, m = f"TRY_CAST(s.{col} AS DOUBLE)", f"CAST(m.{col} AS DOUBLE)"
        eq = f"abs({s} - {m}) <= 1e-9 * greatest(1.0, abs({s}))"
    elif boolean:
        s, m = f"lower(s.{col})", f"lower(CAST(m.{col} AS VARCHAR))"
        eq = f"{s} = {m}"
    elif col == "created_at":
        s, m = "s.created_at", "CAST(CAST(m.created_at AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS DATE)::VARCHAR"
        eq = f"{s} = {m}"
    else:
        s, m = f"s.{col}", f"CAST(m.{col} AS VARCHAR)"
        eq = f"{s} = {m}"
    return s, m, eq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", required=True, help="YYYY-MM-DD; ACCESSAVA created_at <= cutoff")
    ap.add_argument("--sf-cache", help="optional parquet cache of the ACCESSAVA slice (not committed)")
    ap.add_argument("--form-csv", help="CSV export of ascFuncs inst/extdata/demographics_matched.RDS (transcript_id, "
                    "ethnicity, age_range, person_type, disability_n_y, armed_service); explains ACCESSAVA values that "
                    "process_raw_data coalesces in from the external equalities form")
    ap.add_argument("--out", help="markdown summary path")
    a = ap.parse_args()

    c = duckdb.connect()
    c.execute(f"ATTACH '{MD}' AS md")
    pull_accessava(c, a.cutoff, a.sf_cache)
    c.execute(f"CREATE VIEW sfc AS SELECT * FROM sf WHERE CAST(created_at AS DATE) <= DATE '{a.cutoff}'")
    n_sf = c.execute("SELECT COUNT(*) FROM sfc").fetchone()[0]
    if a.form_csv:
        c.execute(f"CREATE TABLE form AS SELECT * FROM read_csv('{a.form_csv}', header=true, all_varchar=true)")

    md = ["# Parity: chatbot feature models, group A (int_chatbot_*)", "",
          f"- Date: {date.today().isoformat()}  |  frozen cutoff vs ACCESSAVA: **{a.cutoff}** ({n_sf} ACCESSAVA transcripts)",
          "- Oracle: Snowflake AVA.PUBLIC.ACCESSAVA (R's joined output, deduplicated by dedupe_by_id). "
          "Each column compared over all ACCESSAVA ids; a transcript missing from the dbt model counts as NULL.",
          "- Cells are counts of ACCESSAVA transcripts. `diff` = both non-NULL and different; "
          "`only SF` / `only dbt` = one side NULL.", ""]
    summary = []
    for model, fn, cols, nums, bools in FEATURES:
        rel = f"{SCHEMA}.{model}"
        n_rows, n_ids, n_dup = c.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT transcript_id), COUNT(*) - COUNT(DISTINCT transcript_id) FROM {rel}").fetchone()
        md += [f"## {fn} -> {model}", "", f"- dbt rows {n_rows}, distinct transcript_id {n_ids}, duplicate rows {n_dup}", "",
               "| column | both NULL | equal | only SF | only dbt | diff |", "|---|---|---|---|---|---|"]
        c.execute(f"CREATE OR REPLACE TEMP TABLE m AS SELECT * FROM {rel}")
        for col in cols:
            s, m, eq = cmp_expr(col, col in nums, col in bools)
            r = c.execute(f"""
                SELECT COUNT(*) FILTER (WHERE s.{col} IS NULL AND {m} IS NULL),
                       COUNT(*) FILTER (WHERE {eq}),
                       COUNT(*) FILTER (WHERE s.{col} IS NOT NULL AND {m} IS NULL),
                       COUNT(*) FILTER (WHERE s.{col} IS NULL AND {m} IS NOT NULL),
                       COUNT(*) FILTER (WHERE s.{col} IS NOT NULL AND {m} IS NOT NULL AND NOT coalesce({eq}, false))
                FROM sfc s LEFT JOIN (SELECT DISTINCT * FROM m) m USING (transcript_id)""").fetchone()
            md.append(f"| {col} | " + " | ".join(str(x) for x in r) + " |")
            summary.append((fn, col, r))
            if a.form_csv and r[2] and col in ("ethnicity", "age_range", "person_type", "disability_n_y", "armed_service"):
                n_form = c.execute(f"""
                    SELECT COUNT(*) FROM sfc s LEFT JOIN (SELECT DISTINCT * FROM m) m USING (transcript_id)
                    JOIN form f ON f.transcript_id = s.transcript_id AND f.{col} IS NOT NULL
                    WHERE s.{col} IS NOT NULL AND {m} IS NULL""").fetchone()[0]
                md += ["", f"`{col}`: of the {r[2]} `only SF` values, {n_form} are transcripts whose value comes from the "
                       "external equalities form (process_raw_data coalesces demographics_matched.RDS in)."]
            if r[4] and col not in FREE_TEXT:
                pairs = c.execute(f"""
                    SELECT s.{col}, {m}, COUNT(*) FROM sfc s LEFT JOIN (SELECT DISTINCT * FROM m) m USING (transcript_id)
                    WHERE s.{col} IS NOT NULL AND {m} IS NOT NULL AND NOT coalesce({eq}, false)
                    GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 8""").fetchall()
                md += ["", f"Top differing values for `{col}` (ACCESSAVA -> dbt, count): "
                       + "; ".join(f"'{x}' -> '{y}' ({n})" for x, y, n in pairs)]
        md.append("")

    text = "\n".join(md) + "\n"
    print(text)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)


if __name__ == "__main__":
    main()
