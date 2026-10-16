# Mart logic check: dbt SQL on Snowflake's own partner history vs live HELPLINES_AGGREGATED_FULL

- Stable periods the dbt SQL finds (top 3): [('2020-01', '2026-05', 77, 5.7)]
- Rows: dbt logic 25673 vs Snowflake mart 24743; sum(n) 676229 vs 655867
- Months: 77 vs 77; months only in one side: []
- Rows only in dbt logic: 1650; rows only in Snowflake mart: 720
- sum(n) difference (dbt minus Snowflake) by month: [('2024-01-31', 1326), ('2024-05-31', 1304), ('2024-04-30', 1289), ('2023-11-30', 1151), ('2024-02-29', 1149), ('2024-07-31', 1130)]
- ... by UT1: [('Information Seeking', 16139), ('Carers', 1169), ('Advocacy', 1075), ('Charging', 1009), ('Assessments', 711), ('Direct payments', -263), ('Care plan', 246), ('Legal issues and complaints', 227), ('Safeguarding', 80)]
- sum(n) on NULL LA_NAME, dbt vs Snowflake: (617522, 599360)
- Verdict: **DIFFERS**
