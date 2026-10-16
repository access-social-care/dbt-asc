# Mart logic check: dbt SQL on Snowflake's own partner history vs live HELPLINES_AGGREGATED_FULL

- Stable periods the dbt SQL finds (top 3): [('2020-01', '2026-05', 77, 5.7)]
- Rows: dbt logic 25673 vs Snowflake mart 24743; sum(n) 676229 vs 655867
- Months: 77 vs 77; months only in one side: []
- Rows only in dbt logic: 1650; rows only in Snowflake mart: 720
- Verdict: **DIFFERS**
