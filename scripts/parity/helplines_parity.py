"""Per-month parity of a DuckDB partner history model against its Snowflake PARTNER_HISTORY_* table.

Extension of parity.py for the helplines migration. Whole-table parity is the wrong
question here: the raw files on SharePoint only cover part of the Snowflake history, so
this reports, per YYYY-MM, whether the two sides hold the same rows.

Per month and per side: row count plus an order-independent hash of the grouped rows
(t1, t2, ut1, ut2, la_name, la_code with their counts), and one hash per column to say
which column differs. Everything is pushed down as aggregates; SELECT only; Snowflake is
read as ROLE_DEV through parity.sf_connect (same JWT auth as ~/.dbt/profiles.yml).

  python scripts/parity/helplines_parity.py --duck staging.main.partner_history_cab \
      --sf HELPLINES.PUBLIC.PARTNER_HISTORY_CAB --out docs/parity/helplines_cab_months.csv

UT1 spellings the aggregator normalises (Information seeking, Assessment, Advocacy/Appropriate
Person), the Scope sentinel '999.0' and a literal 'NA' in UT2 (history written before the CSV map, 2026-09-17) are compared
normalised on both sides; a PARITY month whose raw UT labels still differ is flagged in the last
column as map vintage drift (Snowflake history is never retagged when the map changes).

Verdict per month: PARITY | UT_MAP_DRIFT (t1, t2, la_name, la_code identical, only the topic tags differ:
the universal themes map changed after Snowflake tagged those rows) | VALUES_DIFFER (same count, columns listed) | COUNT_DIFFERS |
ONLY_SNOWFLAKE (month missing from DuckDB) | ONLY_DUCKDB.
Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE.
"""
import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parity as p  # noqa: E402

if not os.environ.get("MOTHERDUCK_TOKEN"):
    raise SystemExit("MOTHERDUCK_TOKEN not set")  # never fall back to a browser login


COLS = ["t1", "t2", "ut1", "ut2", "la_name", "la_code"]


def sql(kind, table):
    """One row per (month, 'all' | column): row count and an order-independent hash of the grouped rows."""
    if kind == "sf":
        h = "TO_NUMBER(SUBSTR(MD5({x}), 1, 8), 'XXXXXXXX')"
        num = "NUMBER(38,0)"
        m = "TO_CHAR(\"DATE\", 'YYYY-MM')"
        cn = lambda c: f'"{c.upper()}"'
    else:
        h = "CAST(('0x' || SUBSTR(MD5({x}), 1, 8)) AS BIGINT)"
        num = "HUGEINT"
        m = "strftime(\"date\", '%Y-%m')"
        cn = lambda c: f'"{c}"'
    def nv(c, raw=False):
        x = cn(c)
        if not raw and c == "ut1":  # label spellings the aggregator normalises anyway
            x = (f"CASE WHEN {x} = 'Advocacy/Appropriate Person' THEN 'Advocacy' WHEN {x} = 'Assessment' THEN 'Assessments' "
                 f"WHEN {x} = 'Information seeking' THEN 'Information Seeking' ELSE {x} END")
        if c == "t2":  # Scope history stores the sentinel as '999.0' (numeric round trip in R); same value
            x = f"CASE WHEN {x} = '999.0' THEN '999' ELSE {x} END"
        if not raw and c == "ut2":  # literal 'NA' predates the CSV map (2026-09-17); R now writes NULL
            x = f"NULLIF({x}, 'NA')"
        return f"COALESCE(CAST({x} AS VARCHAR), '~N~')"

    def part(cols, label, raw=False):
        sel = ", ".join(f"{nv(c, raw)} AS {c}" for c in cols)
        key = " || '|' || ".join(cols)
        hx = h.format(x=f"({key}) || '#' || CAST(n AS VARCHAR)")
        return (f"SELECT m, SUM(n) AS n, CAST(SUM({hx}) AS {num}) AS hv, '{label}' AS k "
                f"FROM (SELECT {m} AS m, {sel}, COUNT(*) AS n FROM {table} GROUP BY ALL) GROUP BY m")

    return " UNION ALL ".join([part(COLS, "all")] + [part([c], c) for c in COLS] + [part(["ut1", "ut2"], "ut_raw", True), part(["t1", "t2", "la_name", "la_code"], "core")])


def fetch(run, kind, table):
    out = {}
    for m, n, hv, k in run(sql(kind, table)):
        out.setdefault(m, {"n": n})[k] = str(hv)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duck", required=True)
    ap.add_argument("--sf", required=True)
    ap.add_argument("--out")
    a = ap.parse_args()
    p.check_sf_name(a.sf)
    dcon, scon = p.duck_connect(), p.sf_connect()

    def drun(s):
        p.guard(s)
        return dcon.execute(s).fetchall()

    def srun(s):
        p.guard(s)
        cur = scon.cursor()
        try:
            cur.execute(s)
            return cur.fetchall()
        finally:
            cur.close()

    D, S = fetch(drun, "duck", a.duck), fetch(srun, "sf", a.sf.upper())
    rows, tally = [], {}
    for m in sorted(set(D) | set(S), key=lambda x: (x is None, x)):
        d, s = D.get(m), S.get(m)
        if d is None:
            v, why = "ONLY_SNOWFLAKE", ""
        elif s is None:
            v, why = "ONLY_DUCKDB", ""
        elif d["n"] != s["n"]:
            v, why = "COUNT_DIFFERS", ""
        elif d["all"] == s["all"]:
            v, why = "PARITY", ("map_vintage_drift_in_ut1_ut2_spelling" if d["ut_raw"] != s["ut_raw"] else "")
        elif d["core"] == s["core"]:
            v, why = "UT_MAP_DRIFT", ",".join(c for c in ("ut1", "ut2") if d[c] != s[c])
        else:
            v, why = "VALUES_DIFFER", ",".join(c for c in COLS if d[c] != s[c])
        tally[v] = tally.get(v, 0) + 1
        rows.append([m, v, d["n"] if d else "", s["n"] if s else "", why])
    nd = sum(int(r[2] or 0) for r in rows)
    ns = sum(int(r[3] or 0) for r in rows)
    print(f"{a.duck} vs {a.sf.upper()}: rows duck={nd} sf={ns}; months {tally}")
    for r in rows:
        if os.environ.get("PARITY_VERBOSE") and r[1] not in ("PARITY", "ONLY_SNOWFLAKE"):
            print("  ", r)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["month", "verdict", "duck_rows", "sf_rows", "differing_columns"])
            w.writerows(rows)


if __name__ == "__main__":
    main()
