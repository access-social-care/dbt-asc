{{ la_legal_letters(months_back=1,  source_model='stg_la_queries_glos') }}
UNION ALL
{{ la_legal_letters(months_back=3,  source_model='stg_la_queries_glos') }}
UNION ALL
{{ la_legal_letters(months_back=6,  source_model='stg_la_queries_glos') }}
UNION ALL
{{ la_legal_letters(months_back=9,  source_model='stg_la_queries_glos') }}
UNION ALL
{{ la_legal_letters(months_back=12, source_model='stg_la_queries_glos') }}
