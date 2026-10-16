"""Does the dbt mart logic reproduce HELPLINES_AGGREGATED_FULL when fed Snowflake's own history?

The month-level parity of the partner models is limited by what is still on SharePoint. This
isolates the mart: it copies the Snowflake PARTNER_HISTORY_* tables (as grouped aggregates, then
expanded locally), runs the compiled dbt SQL of helplines_stable_period and helplines_ongoing
against them in an in-memory DuckDB, and compares the result to the live Snowflake mart.
Snowflake is read-only (ROLE_DEV, SELECT). Nothing is written to MotherDuck.

Run `dbt compile --target duckdb --select helplines_stable_period helplines_ongoing` first.
  python scripts/parity/helplines_mart_check.py [--out docs/parity/helplines_mart_check.md]
"""
import argparse
import os
import sys

import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parity as p  # noqa: E402

if not os.environ.get("MOTHERDUCK_TOKEN"):
    raise SystemExit("MOTHERDUCK_TOKEN not set")  # never fall back to a browser login


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PARTNERS = ["IA", "CAB", "EALING", "AGEUK", "CARERSUK", "SCOPE", "MENCAP", "RNIB"]


T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:6.0f}s] {msg}", file=sys.stderr, flush=True)


def compiled(path):
    with open(os.path.join(ROOT, "target", "compiled", "asc", "models", path), encoding="utf-8") as f:
        return f.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    a = ap.parse_args()
    scon = p.sf_connect()

    def srun(sql):
        p.guard(sql)
        cur = scon.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    loc = duckdb.connect(":memory:")
    loc.execute("ATTACH ':memory:' AS analytics_dev")
    loc.execute("CREATE SCHEMA analytics_dev.staging")
    loc.execute("CREATE SCHEMA analytics_dev.marts")
    loc.execute("ATTACH ':memory:' AS reference")
    # la_to_cassr lives in MotherDuck reference: take it read-only
    md = p.duck_connect()
    rows = md.execute("SELECT la_name, cassr_name FROM reference.main.la_to_cassr").fetchall()
    loc.execute("CREATE TABLE reference.main.la_to_cassr (la_name VARCHAR, cassr_name VARCHAR)")
    loc.executemany("INSERT INTO reference.main.la_to_cassr VALUES (?, ?)", rows)
    loc.execute("CREATE TABLE analytics_dev.staging.helplines_partner_history "
                "(date DATE, month VARCHAR, t1 VARCHAR, t2 VARCHAR, ut1 VARCHAR, ut2 VARCHAR, la_name VARCHAR, la_code VARCHAR, org VARCHAR)")
    for part in PARTNERS:
        g = srun(f'SELECT "DATE", "MONTH", "UT1", "LA_NAME", "LA_CODE", "ORG", COUNT(*) FROM HELPLINES.PUBLIC.PARTNER_HISTORY_{part} GROUP BY ALL')
        log(f"{part}: {len(g)} grouped rows from Snowflake")
        tmp = f"grouped_{part.lower()}"
        loc.execute(f"CREATE TEMP TABLE {tmp} (d DATE, m VARCHAR, ut1 VARCHAR, la_name VARCHAR, la_code VARCHAR, org VARCHAR, n BIGINT)")
        for i in range(0, len(g), 400):  # multi-row inserts: executemany is far too slow here
            chunk = g[i:i + 400]
            loc.execute(f"INSERT INTO {tmp} VALUES " + ",".join(["(?,?,?,?,?,?,?)"] * len(chunk)),
                        [v for row in chunk for v in row])
        loc.execute("INSERT INTO analytics_dev.staging.helplines_partner_history "
                    f"SELECT d, m, NULL, NULL, ut1, NULL, la_name, la_code, org FROM {tmp}, range({tmp}.n)")
    log("history expanded")
    loc.execute("CREATE TABLE analytics_dev.staging.helplines_stable_period AS " + compiled("staging/helplines/helplines_stable_period.sql"))
    loc.execute("CREATE TABLE analytics_dev.marts.helplines_ongoing AS " + compiled("marts/helplines/helplines_ongoing.sql"))
    log("mart built")
    period = loc.execute("SELECT * FROM analytics_dev.staging.helplines_stable_period LIMIT 3").fetchall()

    mine = loc.execute("SELECT month_date, ut1, la_name, n FROM analytics_dev.marts.helplines_ongoing").fetchall()
    sf = srun("SELECT MONTH_DATE, UT1, LA_NAME, N FROM HELPLINES.PUBLIC.HELPLINES_AGGREGATED_FULL")
    key = lambda r: (str(r[0]), r[1], r[2] or "~N~", float(r[3]))
    A, B = sorted(map(key, mine)), sorted(map(key, sf))
    sa, sb = {}, {}
    for r in A:
        sa[r] = sa.get(r, 0) + 1
    for r in B:
        sb[r] = sb.get(r, 0) + 1
    only_a = sum(max(v - sb.get(k, 0), 0) for k, v in sa.items())
    only_b = sum(max(v - sa.get(k, 0), 0) for k, v in sb.items())
    months_a, months_b = {r[0][:7] for r in A}, {r[0][:7] for r in B}
    text = [
        "# Mart logic check: dbt SQL on Snowflake's own partner history vs live HELPLINES_AGGREGATED_FULL", "",
        f"- Stable periods the dbt SQL finds (top 3): {period}",
        f"- Rows: dbt logic {len(A)} vs Snowflake mart {len(B)}; sum(n) {sum(r[3] for r in A):.0f} vs {sum(r[3] for r in B):.0f}",
        f"- Months: {len(months_a)} vs {len(months_b)}; months only in one side: {sorted(months_a ^ months_b)[:12]}",
        f"- Rows only in dbt logic: {only_a}; rows only in Snowflake mart: {only_b}",
        f"- Verdict: **{'PASS' if only_a == 0 and only_b == 0 else 'DIFFERS'}**",
    ]
    print("\n".join(text))
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(text) + "\n")


if __name__ == "__main__":
    main()
