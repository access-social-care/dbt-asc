"""Parity for the chatbot base layer (stg_chatbot_transcript, stg_chatbot_transcript_event).

Two oracles, both read-only:
  1. Snowflake AVA.PUBLIC.ACCESSAVA (what the R pipeline finally produces), compared at a frozen
     cutoff on distinct transcript_id sets and on conversations per tenant per month.
     The base-derived conversation set mirrors chatbot_total_convos: first event per
     transcript from stg_chatbot_transcript_event, plus transcripts with a non-empty
     raw_tracker_record from stg_chatbot_transcript (tr values win, as in R), restricted to
     is_reporting_tenant (the report.Rmd step that builds data.csv).
  2. Optional: the R pipeline's own tr / tr_ev output (cached RDS files exported to CSV by
     scripts/parity/export_r_cache.R), compared row-for-row on a cutoff the cache covers.

Snowflake is SELECT-only (ROLE_DEV). Only ids, tenant names and dates are pulled; nothing is
written to Snowflake or MotherDuck.

Env: MOTHERDUCK_TOKEN, SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE (never printed).

Example:
  python scripts/parity/chatbot_base_parity.py --cutoff 2026-10-13 ^
      --r-tr scratch/r_tr.csv --r-trev scratch/r_trev.csv --r-cutoff 2026-02-18 ^
      --out docs/parity/chatbot_base.md
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
TR = "md.staging.stg_chatbot_transcript"
EV = "md.staging.stg_chatbot_transcript_event"


def table(rows, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", required=True, help="YYYY-MM-DD; entry_day <= cutoff compared against ACCESSAVA")
    ap.add_argument("--r-tr", help="CSV export of the R cache actions_transcript.RDS")
    ap.add_argument("--r-trev", help="CSV export of the R cache actions_transcript_events.RDS")
    ap.add_argument("--r-cutoff", help="YYYY-MM-DD exclusive created_at bound for the R-cache comparison")
    ap.add_argument("--out", help="markdown summary path")
    a = ap.parse_args()

    c = duckdb.connect()
    c.execute(f"ATTACH '{MD}' AS md")

    # --- Snowflake ACCESSAVA slim copy (ids, dates, tenant only) ---
    scon = parity.sf_connect()
    cur = scon.cursor()
    sql = "SELECT transcript_id, created_at, tenant_name FROM ava.public.accessava"
    parity.guard(sql)
    cur.execute(sql)
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["transcript_id", "created_at", "tenant_name"])
        w.writerows(cur.fetchall())
        sf_csv = f.name
    scon.close()
    c.execute(f"CREATE TABLE sf AS SELECT * FROM read_csv('{sf_csv}', header=true, "
              f"types={{'transcript_id':'VARCHAR','created_at':'DATE','tenant_name':'VARCHAR'}}) "
              f"WHERE created_at <= DATE '{a.cutoff}'")
    os.unlink(sf_csv)

    c.execute(f"""CREATE TABLE base_ev AS
        SELECT transcript_id, arg_min(entry_day, created_at) AS d, arg_min(tenant_name, created_at) AS t
        FROM {EV} WHERE entry_day <= DATE '{a.cutoff}' AND is_reporting_tenant GROUP BY 1""")
    c.execute(f"""CREATE TABLE base_tr AS
        SELECT id AS transcript_id, entry_day AS d, tenant_name AS t
        FROM {TR} WHERE entry_day <= DATE '{a.cutoff}' AND is_reporting_tenant AND is_engaged""")
    c.execute("""CREATE TABLE base_u AS
        SELECT transcript_id, COALESCE(tr.d, ev.d) AS d, COALESCE(tr.t, ev.t) AS t
        FROM base_ev ev FULL JOIN base_tr tr USING (transcript_id)""")

    one = lambda s: c.execute(s).fetchone()  # noqa: E731
    md = [f"# Parity: chatbot base layer (stg_chatbot_transcript, stg_chatbot_transcript_event)", "",
          f"- Date: {date.today().isoformat()}  |  frozen entry_day cutoff vs ACCESSAVA: **{a.cutoff}**", ""]

    # --- A. vs Snowflake ACCESSAVA ---
    n_sf, n_ev, n_tr, n_u = (one("SELECT COUNT(*) FROM sf")[0], one("SELECT COUNT(*) FROM base_ev")[0],
                             one("SELECT COUNT(*) FROM base_tr")[0], one("SELECT COUNT(*) FROM base_u")[0])
    only_sf = one("SELECT COUNT(*) FROM sf WHERE transcript_id NOT IN (SELECT transcript_id FROM base_u)")[0]
    only_b = one("SELECT COUNT(*) FROM base_u WHERE transcript_id NOT IN (SELECT transcript_id FROM sf)")[0]
    both = one("SELECT COUNT(*) FROM sf JOIN base_u USING (transcript_id)")[0]
    ten_diff, day_diff = one("""SELECT COUNT(*) FILTER (WHERE s.tenant_name IS DISTINCT FROM b.t),
                                       COUNT(*) FILTER (WHERE s.created_at <> b.d)
                                FROM sf s JOIN base_u b USING (transcript_id)""")
    cells = c.execute("""
        WITH s AS (SELECT strftime(created_at, '%Y-%m') AS m, tenant_name AS t, COUNT(*) AS n FROM sf GROUP BY 1, 2),
             b AS (SELECT strftime(d, '%Y-%m') AS m, t, COUNT(*) AS n FROM base_u GROUP BY 1, 2)
        SELECT COALESCE(s.m, b.m), COALESCE(s.t, b.t), COALESCE(s.n, 0), COALESCE(b.n, 0)
        FROM s FULL JOIN b ON s.m = b.m AND s.t = b.t""").fetchall()
    bad = [r for r in cells if r[2] != r[3]]
    md += ["## A. Base vs Snowflake AVA.PUBLIC.ACCESSAVA", "",
           f"- ACCESSAVA conversations (created_at <= cutoff): {n_sf}",
           f"- Base-derived conversations: {n_u} (distinct transcripts in stg_chatbot_transcript_event: {n_ev}; "
           f"engaged transcripts in stg_chatbot_transcript: {n_tr}; reporting tenants only)",
           f"- transcript_id in ACCESSAVA only: **{only_sf}**; in base only: **{only_b}**; in both: {both}",
           f"- On common ids: tenant_name differs {ten_diff}; created_at date differs {day_diff}",
           f"- Tenant x month cells: {len(cells)} compared, {len(cells) - len(bad)} equal, {len(bad)} differ", ""]
    if bad:
        md += ["Differing cells (month, tenant, ACCESSAVA, base):", ""] + table(sorted(bad)[:40], ["month", "tenant", "accessava", "base"]) + [""]
    if only_b:
        rows = c.execute("""SELECT t, strftime(d, '%Y-%m') AS m, COUNT(*) FROM base_u
                            WHERE transcript_id NOT IN (SELECT transcript_id FROM sf) GROUP BY 1, 2 ORDER BY 2, 1""").fetchall()
        md += ["Base-only transcripts by tenant and month:", ""] + table(rows, ["tenant", "month", "n"]) + [""]
    if only_sf:
        rows = c.execute("""SELECT tenant_name, strftime(created_at, '%Y-%m'), COUNT(*) FROM sf
                            WHERE transcript_id NOT IN (SELECT transcript_id FROM base_u) GROUP BY 1, 2 ORDER BY 2, 1""").fetchall()
        md += ["ACCESSAVA-only transcripts by tenant and month:", ""] + table(rows, ["tenant", "month", "n"]) + [""]

    # --- B. vs the R pipeline's own tr / tr_ev ---
    if a.r_tr and a.r_trev and a.r_cutoff:
        cut = a.r_cutoff
        c.execute(f"CREATE TABLE r_tr AS SELECT * FROM read_csv_auto('{a.r_tr}', all_varchar=true) WHERE created_at < '{cut}'")
        c.execute(f"CREATE TABLE r_ev AS SELECT * FROM read_csv_auto('{a.r_trev}', all_varchar=true) WHERE created_at < '{cut}'")
        c.execute(f"""CREATE TABLE d_tr AS SELECT id, entry_day::VARCHAR AS ed, entry_month, entry_week, tenant_name,
                      raw_tracker_record = '[]' AS empty_tracker
                      FROM {TR} WHERE created_at < TIMESTAMPTZ '{cut} 00:00:00+00'""")
        c.execute(f"""CREATE TABLE d_ev AS SELECT id::VARCHAR AS id, transcript_id, entry_day::VARCHAR AS ed, entry_month,
                      entry_week, tenant_name, contentful_id, lettercode
                      FROM {EV} WHERE created_at < TIMESTAMPTZ '{cut} 00:00:00+00'""")
        n_rtr, n_dtr = one("SELECT COUNT(*) FROM r_tr")[0], one("SELECT COUNT(*) FROM d_tr")[0]
        r_only_tr = one("SELECT COUNT(*) FROM r_tr WHERE id NOT IN (SELECT id FROM d_tr)")[0]
        d_only_tr, d_only_empty = one("""SELECT COUNT(*), COUNT(*) FILTER (WHERE empty_tracker) FROM d_tr
                                         WHERE id NOT IN (SELECT id FROM r_tr)""")
        f_tr = one("""SELECT COUNT(*) FILTER (WHERE r.entry_month <> d.entry_month), COUNT(*) FILTER (WHERE r.entry_week <> d.entry_week),
                             COUNT(*) FILTER (WHERE r.entry_day <> d.ed), COUNT(*) FILTER (WHERE r.tenant_name IS DISTINCT FROM d.tenant_name)
                      FROM r_tr r JOIN d_tr d USING (id)""")
        tn_tr = c.execute("""SELECT r.tenant_name, d.tenant_name, COUNT(*) FROM r_tr r JOIN d_tr d USING (id)
                             WHERE r.tenant_name IS DISTINCT FROM d.tenant_name GROUP BY 1, 2""").fetchall()
        n_rev, n_dev = one("SELECT COUNT(*) FROM r_ev")[0], one("SELECT COUNT(*) FROM d_ev")[0]
        r_only_ev = one("SELECT COUNT(*) FROM r_ev WHERE id NOT IN (SELECT id FROM d_ev)")[0]
        d_only_ev = one("SELECT COUNT(*) FROM d_ev WHERE id NOT IN (SELECT id FROM r_ev)")[0]
        t_r, t_d = one("SELECT COUNT(DISTINCT transcript_id) FROM r_ev")[0], one("SELECT COUNT(DISTINCT transcript_id) FROM d_ev")[0]
        f_ev = one("""SELECT COUNT(*) FILTER (WHERE r.entry_month <> d.entry_month), COUNT(*) FILTER (WHERE r.entry_week <> d.entry_week),
                             COUNT(*) FILTER (WHERE r.entry_day <> d.ed), COUNT(*) FILTER (WHERE r.tenant_name IS DISTINCT FROM d.tenant_name),
                             COUNT(*) FILTER (WHERE r.contentful_id IS DISTINCT FROM d.contentful_id AND NOT (r.contentful_id = 'null' AND d.contentful_id IS NULL)),
                             COUNT(*) FILTER (WHERE r.contentful_id = 'null' AND d.contentful_id IS NULL)
                      FROM r_ev r JOIN d_ev d USING (id)""")
        lc = one("""SELECT COUNT(*) FILTER (WHERE r.lettercode NOT IN ('NA') AND d.lettercode IS NOT NULL AND r.lettercode = d.lettercode),
                           COUNT(*) FILTER (WHERE r.lettercode NOT IN ('NA') AND d.lettercode IS NOT NULL AND r.lettercode <> d.lettercode),
                           COUNT(*) FILTER (WHERE r.lettercode NOT IN ('NA') AND d.lettercode IS NULL),
                           COUNT(*) FILTER (WHERE r.lettercode = 'NA' AND d.lettercode IS NOT NULL)
                    FROM r_ev r JOIN d_ev d USING (id)""")
        md += [f"## B. Base vs the R pipeline's own tr / tr_ev (cached RDS, created_at < {cut})", "",
               "### stg_chatbot_transcript vs chatbot_process_tr output", "",
               f"- Rows: R {n_rtr}, dbt {n_dtr}; in R only: **{r_only_tr}**; in dbt only: **{d_only_tr}** "
               f"(of which empty-tracker sessions: {d_only_empty})",
               f"- On common ids, mismatches: entry_month {f_tr[0]}, entry_week {f_tr[1]}, entry_day {f_tr[2]}, tenant_name {f_tr[3]}"]
        if tn_tr:
            md += ["- tenant_name differences (R cache vs now): " + "; ".join(f"'{x}' -> '{y}' ({n})" for x, y, n in tn_tr)]
        md += ["", "### stg_chatbot_transcript_event vs chatbot_process_tr_ev output", "",
               f"- Rows: R {n_rev}, dbt {n_dev}; in R only: **{r_only_ev}**; in dbt only: **{d_only_ev}**",
               f"- Distinct transcript_id: R {t_r}, dbt {t_d}",
               f"- On common ids, mismatches: entry_month {f_ev[0]}, entry_week {f_ev[1]}, entry_day {f_ev[2]}, "
               f"tenant_name {f_ev[3]}, contentful_id (other than R text \"null\" vs NULL) {f_ev[4]}",
               f"- contentful_id: R text \"null\" where dbt has NULL on {f_ev[5]} rows (deliberate, see model header)",
               f"- lettercode, rows R labelled non-NA: same {lc[0]}, different {lc[1]}, dbt NULL {lc[2]}; "
               f"R 'NA' but dbt has a code {lc[3]} (the R cache predates the current Contentful content)", ""]

    text = "\n".join(md) + "\n"
    print(text)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)


if __name__ == "__main__":
    main()
