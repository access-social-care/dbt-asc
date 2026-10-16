"""Parity for the chatbot star layer: accessava, bridges, dims, locality, downstream marts.

Oracle: Snowflake AVA.PUBLIC.* (and ANALYTICS marts), SELECT only (ROLE_DEV, JWT key auth).
DuckDB side: analytics_dev.marts.* on MotherDuck, SELECT only. Both sides are pulled as
text into a private temp directory, compared by a local in-memory DuckDB, and the temp
files are removed. Only counts reach stdout / the report: no data rows are printed.

Env: MOTHERDUCK_TOKEN (required, exits when unset), SNOWFLAKE_USER, SNOWFLAKE_KEY_FILE.

  python scripts/parity/chatbot_star_parity.py --cutoff 2026-10-13 --part accessava
  python scripts/parity/chatbot_star_parity.py --cutoff 2026-10-13 --part star
  python scripts/parity/chatbot_star_parity.py --cutoff 2026-10-13 --part locality
  python scripts/parity/chatbot_star_parity.py --cutoff 2026-10-13 --part marts
  add --out FILE to write the markdown to a file (parts are separate runs; the doc is assembled by hand).
"""
import argparse
import csv
import datetime
import decimal
import os
import shutil
import sys
import tempfile

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parity  # noqa: E402

# (Snowflake object, dbt model, value column, surrogate key column)
STAR = [
    ("PERSONA", "persona", "persona", "persona_id"),
    ("SUPERCATEGORY", "supercategory", "supercategories", "supercategory_id"),
    ("CATEGORY", "category", "categories", "category_id"),
    ("CSI", "csi", "csis", "csi_id"),
    ("TOPIC_ENTRY_POINT", "topic_entry_point", "topic_entry_point", "topic_entry_point_id"),
    ("LETTER_CODE", "letter_code", "lettercode", "letter_code_id"),
    ("CONSUMED_RESOURCE", "consumed_resource", "consumed_resources", "consumed_resource_id"),
]
# Snowflake type codes: 0 FIXED, 1 REAL, 3 DATE, 13 BOOLEAN; everything else is compared as text.
NUM_CODES = {0, 1}


def ser(v):
    if v is None:
        return "\\N"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, (float, decimal.Decimal)):
        return repr(float(v))
    return str(v)


def dump(path, cols, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([ser(x) for x in r])


class Ctx:
    def __init__(self):
        if not os.environ.get("MOTHERDUCK_TOKEN"):
            raise SystemExit("MOTHERDUCK_TOKEN is not set")
        self.tmp = tempfile.mkdtemp(prefix="star_parity_")
        self.sf = parity.sf_connect()
        self.md = duckdb.connect("md:analytics_dev")
        self.local = duckdb.connect()
        self.n = 0

    def close(self):
        self.sf.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def pull(self, sf_sql, md_sql):
        """Return (sf_csv, md_csv, cols, num_cols) for two SELECTs with the same lower-case column list."""
        parity.guard(sf_sql)
        parity.guard(md_sql)
        cur = self.sf.cursor()
        cur.execute(sf_sql)
        cols = [d[0].lower() for d in cur.description]
        num = {d[0].lower() for d in cur.description if d[1] in NUM_CODES}
        self.n += 1
        a = os.path.join(self.tmp, f"sf{self.n}.csv")
        dump(a, cols, cur.fetchall())
        r = self.md.execute(md_sql)
        mcols = [d[0].lower() for d in r.description]
        if mcols != cols:
            raise SystemExit(f"column mismatch: {cols} vs {mcols}")
        b = os.path.join(self.tmp, f"md{self.n}.csv")
        dump(b, cols, r.fetchall())
        return a, b, cols, num

    def compare(self, sf_sql, md_sql, keys):
        a, b, cols, num = self.pull(sf_sql, md_sql)
        rd = "read_csv('{}', all_varchar=true, header=true, nullstr='\\N', quote='\"', escape='\"')"
        s, d = rd.format(a.replace("\\", "/")), rd.format(b.replace("\\", "/"))
        on = " and ".join(f"s.{k} = d.{k}" for k in keys)
        k0 = keys[0]
        head = self.local.execute(f"""
            select count(*) filter (where s.{k0} is not null and d.{k0} is not null),
                   count(*) filter (where d.{k0} is null), count(*) filter (where s.{k0} is null)
            from {s} s full join {d} d on {on}""").fetchone()
        res = {"both": head[0], "only_sf_rows": head[1], "only_dbt_rows": head[2], "cols": []}
        for c in cols:
            if c in keys:
                continue
            eq = (f"abs(try_cast(s.{c} as double) - try_cast(d.{c} as double)) <= 1e-9" if c in num
                  else f"s.{c} = d.{c}")
            r = self.local.execute(f"""
                select count(*) filter (where s.{c} is null and d.{c} is null),
                       count(*) filter (where s.{c} is not null and d.{c} is not null and ({eq})),
                       count(*) filter (where s.{c} is not null and d.{c} is not null and not ({eq})),
                       count(*) filter (where s.{c} is not null and d.{c} is null),
                       count(*) filter (where s.{c} is null and d.{c} is not null)
                from {s} s join {d} d on {on}""").fetchone()
            res["cols"].append((c, *r))
        return res, (a, b)


def table_md(title, res):
    out = [f"### {title}", "",
           f"- rows in both: {res['both']}; only Snowflake: {res['only_sf_rows']}; only dbt: {res['only_dbt_rows']}", ""]
    if res["cols"]:
        out += ["| column | both NULL | equal | diff | only SF | only dbt |", "|---|---|---|---|---|---|"]
        out += [f"| {c} | {a} | {b} | {d} | {e} | {f} |" for c, a, b, d, e, f in res["cols"]]
        out.append("")
    return out


def verdict(res):
    bad = res["only_sf_rows"] or res["only_dbt_rows"] or any(r[3] or r[4] or r[5] for r in res["cols"])
    return "DIFFERS" if bad else "PARITY"


def part_accessava(x, cutoff):
    cols = ("transcript_id, created_at, tenant_name, engaged, region, county, organisation_type, completed, ethnicity, "
            "age_range, person_type, disability_n_y, disability_text, armed_service, la_name, dif, churn_point, "
            "churn_category, rating, user_feedback, persona, supercategories, categories, csis, lettercode, "
            "letter_downloaded, autocomplete_start, autocomplete_finish, factsheets_templates, rasa, "
            "topic_entry_point, consumed_resources, age, referring_page_url, referring_page_parameters, entry_month")
    res, _ = x.compare(f"select {cols} from ava.public.accessava where created_at <= date '{cutoff}'",
                       f"select {cols} from marts.accessava where created_at <= date '{cutoff}'", ["transcript_id"])
    return ["## ACCESSAVA", ""] + table_md(f"accessava vs AVA.PUBLIC.ACCESSAVA (created_at <= {cutoff}): {verdict(res)}", res), res


def part_star(x, cutoff):
    md = ["## Bridges and dimensions", "", "| object | verdict | rows both | only SF | only dbt | note |", "|---|---|---|---|---|---|"]
    sf_ids = f"select transcript_id from ava.public.accessava where created_at <= date '{cutoff}'"
    md_ids = f"select transcript_id from marts.accessava where created_at <= date '{cutoff}'"
    detail = []
    for sfname, model, val, key in STAR:
        b_sf = (f"select transcript_id, {val}, row_number() over (partition by transcript_id, {val} order by 1) as rn "
                f"from ava.public.{sfname}_bridge where transcript_id in ({sf_ids})")
        b_md = (f"select transcript_id, {val}, row_number() over (partition by transcript_id, {val} order by 1) as rn "
                f"from marts.{model}_bridge where transcript_id in ({md_ids})")
        r, _ = x.compare(b_sf, b_md, ["transcript_id", val, "rn"])
        md.append(f"| {sfname}_BRIDGE | {verdict(r)} | {r['both']} | {r['only_sf_rows']} | {r['only_dbt_rows']} | cutoff {cutoff} |")
        d_sf = f"select {key}, {val} from ava.public.{sfname}_dim"
        d_md = f"select {key}, {val} from marts.{model}_dim"
        r2, _ = x.compare(d_sf, d_md, [val])
        idd = r2["cols"][0] if r2["cols"] else None
        note = f"surrogate key diff {idd[3]}" if idd else ""
        md.append(f"| {sfname}_DIM | {verdict(r2)} | {r2['both']} | {r2['only_sf_rows']} | {r2['only_dbt_rows']} | {note} |")
        detail.append((sfname, r, r2))
    return md + [""], detail


def part_locality(x, cutoff):
    cols = ("transcript_id, la_name, local_authority_code, ward, ward_code, constituency_name, constituency_code, "
            "mso_area_name, mso_area_code, lso_area_name, lso_area_code, nhs_icb_name, nhs_icb_code, "
            "urban_rural_classification_name, urban_rural_classification_code, "
            "urban_conurbation_classification_name, urban_conurbation_classification_code, county, county_code")
    res, _ = x.compare(
        f"select {cols} from ava.public.accessava_locality where transcript_id in "
        f"(select transcript_id from ava.public.accessava where created_at <= date '{cutoff}')",
        f"select {cols} from marts.accessava_locality where transcript_id in "
        f"(select transcript_id from marts.accessava where created_at <= date '{cutoff}')", ["transcript_id"])
    return ["## Locality", ""] + table_md(f"accessava_locality vs AVA.PUBLIC.ACCESSAVA_LOCALITY (transcripts in ACCESSAVA at {cutoff}): {verdict(res)}", res), res


def hashed(cols, table, where):
    """Multiset key for a table without a unique key: md5 of all columns plus an occurrence number."""
    expr = " || '|' || ".join(f"coalesce(cast({c} as varchar), '~N~')" for c in cols)
    return (f"select md5({expr}) as k, row_number() over (partition by md5({expr}) order by 1) as rn "
            f"from {table} where {where}")


def part_marts(x, cutoff):
    md = ["## Downstream models", ""]
    out = []
    # Tenant marts: the two mart tables are not built in Snowflake today (no object in ANALYTICS), so the
    # oracle is the model SQL run on AVA.PUBLIC.ACCESSAVA in Snowflake. Months before the cutoff month only.
    first = cutoff[:7] + "-01"
    sf_m = ("select tenant_name, date_trunc('month', created_at) as conversation_month, count(*) as conversation_count, "
            "count(distinct transcript_id) as unique_conversations, min(created_at) as first_conversation_date, "
            "max(created_at) as last_conversation_date from ava.public.accessava where tenant_name is not null "
            f"group by 1, 2 having date_trunc('month', created_at) < date '{first}'")
    md_m = ("select tenant_name, cast(conversation_month as date) as conversation_month, conversation_count, unique_conversations, first_conversation_date, "
            f"last_conversation_date from marts.mart_chatbot_conversations_by_tenant_monthly where conversation_month < date '{first}'")
    r, _ = x.compare(sf_m, md_m, ["tenant_name", "conversation_month"])
    md += table_md(f"mart_chatbot_conversations_by_tenant_monthly vs model SQL on AVA.PUBLIC.ACCESSAVA (months before {first}): {verdict(r)}", r)
    out.append(("mart_chatbot_conversations_by_tenant_monthly", r))
    sf_t = ("select tenant_name, count(*) as total_conversations, count(distinct transcript_id) as unique_conversations, "
            "min(created_at) as first_conversation_date, max(created_at) as last_conversation_date, "
            "datediff('day', min(created_at), max(created_at)) as days_active from ava.public.accessava "
            f"where tenant_name is not null and created_at <= date '{cutoff}' group by 1")
    md_t = ("select tenant_name, count(*) as total_conversations, count(distinct transcript_id) as unique_conversations, "
            "min(created_at) as first_conversation_date, max(created_at) as last_conversation_date, "
            "date_diff('day', min(created_at), max(created_at)) as days_active from marts.accessava "
            f"where tenant_name is not null and created_at <= date '{cutoff}' group by 1")
    r, _ = x.compare(sf_t, md_t, ["tenant_name"])
    md += table_md(f"tenant_total logic, cutoff {cutoff} (aggregation re-run both sides; the model itself is the same SQL as marts.mart_chatbot_conversations_by_tenant_total): {verdict(r)}", r)
    out.append(("mart_chatbot_conversations_by_tenant_total", r))
    # stg_ilta_accessava: Snowflake table exists (ANALYTICS.PUBLIC_ILTA_STAGING).
    ic = ["record_id", "source_system", "month_date", "la_name", "org_name", "gender", "age", "disability", "ethnicity",
          "gender_raw", "age_raw", "disability_yn_raw", "disability_type_raw", "ethnicity_raw"]
    sf_i = f"select {', '.join(ic)} from analytics.public_ilta_staging.stg_ilta_accessava where record_id in (select transcript_id from ava.public.accessava where created_at <= date '{cutoff}')"
    md_i = f"select {', '.join(ic)} from staging.stg_ilta_accessava where record_id in (select transcript_id from marts.accessava where created_at <= date '{cutoff}')"
    r, _ = x.compare(sf_i, md_i, ["record_id"])
    md += table_md(f"stg_ilta_accessava vs ANALYTICS.PUBLIC_ILTA_STAGING.STG_ILTA_ACCESSAVA (conversations in ACCESSAVA at {cutoff}): {verdict(r)}", r)
    out.append(("stg_ilta_accessava", r))
    # stg_accessava: grain conversation x topic, no unique key; multiset of full-row hashes.
    sc = ["la_name", "query_date", "source_system", "query_count", "segment", "ut2", "age_band", "has_letter", "locality_name"]
    sf_s = hashed(sc, "analytics.public_la_product_staging.stg_accessava", f"query_date <= date '{cutoff}'")
    md_s = hashed(sc, "staging.stg_accessava", f"query_date <= date '{cutoff}'")
    r, _ = x.compare(sf_s, md_s, ["k", "rn"])
    md += table_md(f"stg_accessava vs ANALYTICS.PUBLIC_LA_PRODUCT_STAGING.STG_ACCESSAVA (query_date <= {cutoff}, full-row multiset): {verdict(r)}", r)
    out.append(("stg_accessava", r))
    return md, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", required=True)
    ap.add_argument("--part", required=True, choices=["accessava", "star", "locality", "marts"])
    ap.add_argument("--out")
    a = ap.parse_args()
    x = Ctx()
    try:
        fn = {"accessava": part_accessava, "star": part_star, "locality": part_locality, "marts": part_marts}[a.part]
        md = fn(x, a.cutoff)[0]
    finally:
        x.close()
    text = "\n".join(md) + "\n"
    print(text)
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)


if __name__ == "__main__":
    main()
