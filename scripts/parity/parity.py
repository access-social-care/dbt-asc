"""Parity check: one DuckDB/MotherDuck table vs its Snowflake counterpart.

Snowflake is READ-ONLY (ROLE_DEV, SELECT only, enforced in sf_query). Everything
is pushed down as aggregates; no row data is fetched or written to the report.

Usage (token + Snowflake env vars must already be set, never printed):
  python scripts/parity/parity.py --duck marts.main.norm_cld_assessments \
      --sf EXTERNAL_DATA.NORMALISED.NORM_CLD_ASSESSMENTS \
      --keys area_code,area_unit,breakdown_dimension,breakdown_value,period_start \
      --out docs/parity/norm_cld_assessments.md

Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE (JWT key auth, same
as ~/.dbt/profiles.yml). Snowflake account is read from that profile.

Tolerances: float rel 1e-9; timestamps compared as UTC epoch seconds (floored);
column names case-insensitive; '' vs NULL reported separately (text columns
compare distinct/min/max on NULLIF(col, '')).
"""
import argparse
import json
import math
import os
import re
import sys
from datetime import date

import duckdb

REL_TOL = 1e-9
EXACT_KEY_LIMIT = int(os.environ.get("PARITY_EXACT_KEY_LIMIT", 200_000))  # above this, md5 of sorted keys becomes a hashed checksum
NAME_RE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
OBJECTS_JSON = os.environ.get("SNOWFLAKE_OBJECTS_JSON", r"C:\o\.github\hooks\snowflake_objects.json")


def guard(sql):
    if not re.match(r"^\s*(select|with)\b", sql, re.I):
        raise RuntimeError("parity.py only runs SELECT statements")


def duck_connect():
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        raise SystemExit("MOTHERDUCK_TOKEN unset")
    return duckdb.connect("md:")


def sf_connect():
    import yaml
    import snowflake.connector
    prof = os.path.expanduser("~/.dbt/profiles.yml")
    with open(prof, encoding="utf-8-sig") as f:
        account = yaml.safe_load(f)["asc"]["outputs"]["prod"]["account"]
    return snowflake.connector.connect(
        account=account, user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_KEY_FILE"], authenticator="SNOWFLAKE_JWT",
        role="ROLE_DEV", warehouse="COMPUTE_WH", client_session_keep_alive=False,
        session_parameters={"QUERY_TAG": "parity_readonly"})


def check_sf_name(name):
    if not NAME_RE.match(name):
        sys.exit(f"bad Snowflake name: {name}")
    try:
        with open(OBJECTS_JSON, encoding="utf-8-sig") as f:
            dbs = json.load(f)["databases"]
        if name.split(".")[0].upper() not in dbs:
            sys.exit(f"database not in snowflake_objects.json: {name.split('.')[0]}")
    except FileNotFoundError:
        pass


def klass(t):
    t = t.upper()
    if t.startswith(("TIMESTAMP", "DATETIME")):
        return "ts"
    if t == "DATE":
        return "date"
    if t.startswith("BOOL"):
        return "bool"
    if t.startswith(("NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "INT", "BIGINT", "SMALLINT",
                     "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER", "FIXED")):
        return "num"
    if t.startswith(("TEXT", "VARCHAR", "CHAR", "STRING")):
        return "text"
    return "other"


class Side:
    def __init__(self, kind, run, table):
        self.kind, self.run, self.table = kind, run, table  # kind: 'duck' | 'sf'
        self.cols = {}  # lower name -> (actual name, class)

    def q(self, name):
        return '"' + name.replace('"', '""') + '"'

    def load_columns(self):
        db, sch, tab = self.table.split(".")
        if self.kind == "duck":
            sql = ("SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog='%s' "
                   "AND table_schema='%s' AND table_name='%s' ORDER BY ordinal_position" % (db, sch, tab))
        else:
            sql = ("SELECT column_name, data_type FROM %s.information_schema.columns WHERE table_schema='%s' "
                   "AND table_name='%s' ORDER BY ordinal_position" % (db.upper(), sch.upper(), tab.upper()))
        for n, t in self.run(sql):
            self.cols[n.lower()] = (n, klass(t))
        if not self.cols:
            sys.exit(f"{self.kind}: no columns found for {self.table}")

    def epoch(self, c, k):
        x = self.q(c)
        if self.kind == "sf":
            if k == "date":
                x = f"CAST({x} AS TIMESTAMP_NTZ)"
            return f"FLOOR(DATE_PART(epoch_second, {x}))"
        return f"FLOOR(epoch({x}))"

    def key_expr(self, lc):
        n, k = self.cols[lc]
        x = self.q(n)
        if k in ("ts", "date"):
            v = self.epoch(n, k)
            v = f"CAST({v} AS BIGINT)" if self.kind == "duck" else f"CAST({v} AS NUMBER(38,0))"
            return f"COALESCE(CAST({v} AS VARCHAR), '~N~')"
        return f"COALESCE(CAST({x} AS VARCHAR), '~N~')"

    def profile_sql(self, lcs):
        parts = ["COUNT(*) AS n_rows"]
        dbl = "DOUBLE"
        for i, lc in enumerate(lcs):
            n, k = self.cols[lc]
            x = self.q(n)
            parts.append(f"COUNT(*) - COUNT({x}) AS nul_{i}")
            if k == "text":
                parts.append(f"SUM(CASE WHEN {x} = '' THEN 1 ELSE 0 END) AS emp_{i}")
                y = f"NULLIF({x}, '')"
                parts += [f"COUNT(DISTINCT {y}) AS dst_{i}", f"MIN({y}) AS min_{i}", f"MAX({y}) AS max_{i}"]
            elif k == "num":
                parts += [f"COUNT(DISTINCT {x}) AS dst_{i}", f"MIN(CAST({x} AS {dbl})) AS min_{i}",
                          f"MAX(CAST({x} AS {dbl})) AS max_{i}", f"SUM(CAST({x} AS {dbl})) AS sum_{i}"]
            elif k == "bool":
                parts += [f"COUNT(DISTINCT {x}) AS dst_{i}", f"SUM(CASE WHEN {x} THEN 1 ELSE 0 END) AS sum_{i}"]
            elif k in ("ts", "date"):
                e = self.epoch(n, k)
                parts += [f"COUNT(DISTINCT {e}) AS dst_{i}", f"MIN({e}) AS min_{i}", f"MAX({e}) AS max_{i}"]
            else:
                parts.append(f"COUNT(DISTINCT CAST({x} AS VARCHAR)) AS dst_{i}")
        return f"SELECT {', '.join(parts)} FROM {self.table}"

    def key_sql(self, keys, exact):
        k = " || '|' || ".join(self.key_expr(c) for c in keys)
        if exact:
            agg = "LISTAGG" if self.kind == "sf" else "STRING_AGG"
            inner = (f"{agg}(rk, ',') WITHIN GROUP (ORDER BY rk)" if self.kind == "sf"
                     else "STRING_AGG(rk, ',' ORDER BY rk)")
            return (f"SELECT MD5({inner}) AS h, COUNT(*) AS n, COUNT(DISTINCT rk) AS nd "
                    f"FROM (SELECT {k} AS rk FROM {self.table})")
        if self.kind == "sf":
            h = "TO_NUMBER(SUBSTR(MD5(rk), 1, 8), 'XXXXXXXX')"
            s = f"SUM({h})"
        else:
            h = "CAST(('0x' || SUBSTR(MD5(rk), 1, 8)) AS BIGINT)"
            s = f"CAST(SUM({h}) AS HUGEINT)"
        return (f"SELECT CAST({s} AS VARCHAR) AS h, COUNT(*) AS n, COUNT(DISTINCT rk) AS nd "
                f"FROM (SELECT {k} AS rk FROM {self.table})")


def close(a, b):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=REL_TOL, abs_tol=1e-12)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duck", required=True, help="db.schema.table in MotherDuck")
    ap.add_argument("--sf", required=True, help="DB.SCHEMA.TABLE in Snowflake")
    ap.add_argument("--keys", required=True, help="comma-separated key columns")
    ap.add_argument("--exclude", default="", help="comma-separated columns to skip (case-insensitive)")
    ap.add_argument("--out", help="markdown summary path")
    a = ap.parse_args()
    check_sf_name(a.sf)
    if not NAME_RE.match(a.duck):
        sys.exit("bad DuckDB name")

    dcon, scon = duck_connect(), sf_connect()

    def drun(sql):
        guard(sql)
        return dcon.execute(sql).fetchall()

    def srun(sql):
        guard(sql)
        cur = scon.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    D, S = Side("duck", drun, a.duck), Side("sf", srun, a.sf.upper())
    D.load_columns()
    S.load_columns()
    excl = {c.strip().lower() for c in a.exclude.split(",") if c.strip()}
    keys = [k.strip().lower() for k in a.keys.split(",")]
    common = [c for c in D.cols if c in S.cols and c not in excl]
    only_d = sorted(set(D.cols) - set(S.cols) - excl)
    only_s = sorted(set(S.cols) - set(D.cols) - excl)
    for k in keys:
        if k not in common:
            sys.exit(f"key column {k} missing on one side")

    typed = [c for c in common if D.cols[c][1] == S.cols[c][1]]
    type_diff = [c for c in common if c not in typed]
    # type-mismatched columns: null counts only (class 'other' profile)
    for c in type_diff:
        for side in (D, S):
            side.cols[c] = (side.cols[c][0], "other")

    rows_d = rows_s = None
    CH = 25
    pd_, ps_ = {}, {}
    for i in range(0, len(common), CH):
        chunk = common[i:i + CH]
        rd, rs = drun(D.profile_sql(chunk))[0], srun(S.profile_sql(chunk))[0]
        pd_[i], ps_[i] = (chunk, rd), (chunk, rs)
    # row layout: n_rows, then a per-column group whose stats depend on the column class
    def unpack(side, chunk, row):
        out, p = {}, 1
        n_rows = row[0]
        for lc in chunk:
            k = side.cols[lc][1]
            m = {"text": ["nul", "emp", "dst", "min", "max"], "num": ["nul", "dst", "min", "max", "sum"],
                 "bool": ["nul", "dst", "sum"], "ts": ["nul", "dst", "min", "max"],
                 "date": ["nul", "dst", "min", "max"], "other": ["nul", "dst"]}[k]
            out[lc] = dict(zip(m, row[p:p + len(m)]))
            p += len(m)
        return n_rows, out

    prof_d, prof_s = {}, {}
    for i in pd_:
        rows_d, o = unpack(D, *pd_[i]); prof_d.update(o)
        rows_s, o = unpack(S, *ps_[i]); prof_s.update(o)

    exact = max(rows_d, rows_s) <= EXACT_KEY_LIMIT
    kd, ks = drun(D.key_sql(keys, exact))[0], srun(S.key_sql(keys, exact))[0]

    fails, notes = [], []
    lines = []
    for c in common:
        pdc, psc = prof_d[c], prof_s[c]
        k = D.cols[c][1]
        issues, info = [], []
        if c in type_diff:
            issues.append("type class differs")
        nd_, ns_ = pdc["nul"], psc["nul"]
        if k == "text":
            ed, es = pdc["emp"] or 0, psc["emp"] or 0
            if (nd_, ed) != (ns_, es):
                if nd_ + ed == ns_ + es:
                    info.append(f"EMPTY_VS_NULL (null {nd_} vs {ns_}, empty {ed} vs {es})")
                else:
                    issues.append(f"null/empty {nd_}/{ed} vs {ns_}/{es}")
        elif nd_ != ns_:
            issues.append(f"nulls {nd_} vs {ns_}")
        if pdc["dst"] != psc["dst"]:
            issues.append(f"distinct {pdc['dst']} vs {psc['dst']}")
        for stat in ("min", "max", "sum"):
            if stat in pdc and stat in psc:
                x, y = pdc[stat], psc[stat]
                ok = (x == y) if k in ("text", "ts", "date") else close(x, y)
                if not ok:
                    issues.append(f"{stat} differs" if k == "text" else f"{stat} {x} vs {y}")
        status = "FAIL" if issues else ("NOTE" if info else "ok")
        if issues:
            fails.append(c)
        lines.append(f"| {c} | {k} | {status} | {'; '.join(issues + info)} |")

    key_ok = (kd[0] == ks[0]) and kd[1] == ks[1] and kd[2] == ks[2]
    rows_ok = rows_d == rows_s
    verdict = "PASS" if (rows_ok and key_ok and not fails and not only_d and not only_s) else "FAIL"

    md = [f"# Parity: `{a.duck}` vs `{a.sf.upper()}`", "",
          f"- Date: {date.today().isoformat()}  |  Verdict: **{verdict}**",
          f"- Rows: DuckDB {rows_d} vs Snowflake {rows_s} ({'match' if rows_ok else 'DIFFER'})",
          f"- Key `{', '.join(keys)}`: {'md5 of sorted key set' if exact else 'hashed checksum (sum of md5 prefixes)'} "
          f"{'match' if kd[0] == ks[0] else 'DIFFER'}; distinct keys {kd[2]} vs {ks[2]} "
          f"(duplicate-key rows {kd[1] - kd[2]} vs {ks[1] - ks[2]})",
          f"- Columns compared: {len(common)}; failing: {len(fails)}"
          + (f"; only in DuckDB: {', '.join(only_d)}" if only_d else "")
          + (f"; only in Snowflake: {', '.join(only_s)}" if only_s else ""),
          "- Tolerances: float rel 1e-9, timestamps floored to the second (UTC epoch), names case-insensitive, "
          "'' vs NULL reported as NOTE",
          "", "| column | type | status | detail |", "|---|---|---|---|"] + lines
    text = "\n".join(md) + "\n"
    print(text)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
    sys.exit(0 if verdict == "PASS" else 1)


if __name__ == "__main__":
    main()
