# Parity: `marts.main.norm_cld_long_term_support` vs `EXTERNAL_DATA.NORMALISED.NORM_CLD_LONG_TERM_SUPPORT`

- Date: 2026-10-15  |  Verdict: **PASS**
- Rows: DuckDB 195733 vs Snowflake 195733 (match)
- Key `area_code, area_unit, support_setting, breakdown_dimension, breakdown_value, period_start`: md5 of sorted key set match; distinct keys 195733 vs 195733 (duplicate-key rows 0 vs 0)
- Columns compared: 23; failing: 0
- Tolerances: float rel 1e-9, timestamps floored to the second (UTC epoch), names case-insensitive, '' vs NULL reported as NOTE

| column | type | status | detail |
|---|---|---|---|
| la_code | text | ok |  |
| area_code | text | ok |  |
| area_name | text | ok |  |
| area_unit | text | ok |  |
| area_unit_level | text | ok |  |
| support_setting | text | ok |  |
| support_setting_level | text | ok |  |
| breakdown_dimension | text | ok |  |
| breakdown_value | text | ok |  |
| breakdown_level | text | ok |  |
| measure | text | ok |  |
| period_start | date | ok |  |
| period_end | date | ok |  |
| count_value | num | ok |  |
| population_published | num | ok |  |
| published_population_source | text | ok |  |
| rate_per_100k_published | num | ok |  |
| population_eligible | num | ok |  |
| rate_per_100k_eligible | num | ok |  |
| is_provisional | bool | ok |  |
| publication_date | text | ok |  |
| source_url | text | ok |  |
| run_at | ts | ok |  |
