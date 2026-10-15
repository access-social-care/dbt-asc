# Parity: `analytics_dev.staging.cld_published_population` vs `EXTERNAL_DATA.NORMALISED.CLD_PUBLISHED_POPULATION`

- Date: 2026-10-15  |  Verdict: **PASS**
- Rows: DuckDB 4510 vs Snowflake 4510 (match)
- Key `area_code, dimension, dimension_value, population_base`: md5 of sorted key set match; distinct keys 4510 vs 4510 (duplicate-key rows 0 vs 0)
- Columns compared: 7; failing: 0
- Tolerances: float rel 1e-9, timestamps floored to the second (UTC epoch), names case-insensitive, '' vs NULL reported as NOTE

| column | type | status | detail |
|---|---|---|---|
| area_code | text | ok |  |
| area_name | text | ok |  |
| area_unit | text | ok |  |
| dimension | text | ok |  |
| dimension_value | text | ok |  |
| population_base | text | ok |  |
| population | num | ok |  |
