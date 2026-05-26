{{ la_queries_over_time(months_back=1) }}
UNION ALL
{{ la_queries_over_time(months_back=3) }}
UNION ALL
{{ la_queries_over_time(months_back=6) }}
UNION ALL
{{ la_queries_over_time(months_back=9) }}
UNION ALL
{{ la_queries_over_time(months_back=12) }}
