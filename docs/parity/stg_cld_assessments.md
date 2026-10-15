# Parity: `analytics_dev.staging.stg_cld_assessments` vs `EXTERNAL_DATA.RAW.STG_CLD_ASSESSMENTS`

- Date: 2026-10-15  |  Verdict: **PASS**
- Rows: DuckDB 78390 vs Snowflake 78390 (match)
- Key `area_code, area_name, area_unit, breakdown_dimension, breakdown_value, period_start`: md5 of sorted key set match; distinct keys 78390 vs 78390 (duplicate-key rows 0 vs 0)
- Columns compared: 20; failing: 0
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
| value | num | ok |  |
| is_suppressed | bool | ok |  |
| is_provisional | bool | ok |  |
| metric_raw | text | ok |  |
| publication_date | text | ok |  |
| source_url | text | ok |  |
| run_at | ts | ok |  |
