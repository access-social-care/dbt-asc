{{ config(enabled=(target.type == 'duckdb'), tags=['helplines']) }}

/*
  Canonical partner history: the nine CANONICAL_PARTNER_COLS of every HELPLINES.PUBLIC.PARTNER_HISTORY_*
  table, all eight partners in one table (the per-partner tables remain available as
  partner_history_<partner> for parity). Mencap and RNIB are annual-grain: they are in here but the
  monthly mart and the stable-period test exclude them (ANNUAL_GRAIN_PARTNERS).
*/

select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_ia') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_cab') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_ealing') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_age_uk') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_carers_uk') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_scope') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_mencap') }}
union all select date, month, t1, t2, ut1, ut2, la_name, la_code, org from {{ ref('partner_history_rnib') }}
