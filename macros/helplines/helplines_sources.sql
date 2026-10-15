{#
  Helplines partner files land as one table per file/sheet in raw.share_point
  (Fivetran SharePoint connector). New monthly drops add tables, so models must
  not name them. This macro discovers tables by name prefix at run time and
  unions them, so a new drop is picked up with no code change.

  Arguments
    prefix            table-name prefix, e.g. 'helplines_data_cab_' (starts_with, not LIKE)
    columns           dict  alias -> list of candidate raw column names (first one a
                      table has is used; none => NULL). Layouts drift between drops.
    exclude_contains  list of substrings; matching tables are skipped (e.g. data dictionaries)
    include_contains  optional list; if set a table name must contain at least one
    require_columns   raw column names a table must have to count as a data sheet (skips
                      pivots, dictionaries and other sheets that share the file prefix)

  Every column comes out as VARCHAR (Fivetran types vary per table: the same field is
  VARCHAR in one drop and TIMESTAMPTZ in the next). TIMESTAMPTZ values are rendered as
  the UTC wall clock, which is the Excel serial the R readers saw (readxl reads
  Excel datetimes as UTC). Empty strings become NULL, as readxl reads blank cells.
  Also returned: source_table (file/sheet identity) and source_line (Fivetran _line).
#}
{% macro helplines_sources(prefix, columns, exclude_contains=[], include_contains=none, require_columns=[]) -%}
    {%- set db = var('helplines_raw_database', 'raw') -%}
    {%- set sch = var('helplines_raw_schema', 'share_point') -%}
    {%- set aliases = columns.keys() | list -%}
    {%- set tables = {} -%}
    {%- if execute -%}
        {%- set meta_sql -%}
            select table_name, column_name, data_type
            from information_schema.columns
            where table_catalog = '{{ db }}' and table_schema = '{{ sch }}'
              and starts_with(table_name, '{{ prefix }}')
            order by table_name, ordinal_position
        {%- endset -%}
        {%- for r in run_query(meta_sql).rows -%}
            {%- set skip = namespace(v=false) -%}
            {%- for x in exclude_contains -%}{%- if x in r[0] -%}{%- set skip.v = true -%}{%- endif -%}{%- endfor -%}
            {%- if include_contains is not none -%}
                {%- set hit = namespace(v=false) -%}
                {%- for x in include_contains -%}{%- if x in r[0] -%}{%- set hit.v = true -%}{%- endif -%}{%- endfor -%}
                {%- if not hit.v -%}{%- set skip.v = true -%}{%- endif -%}
            {%- endif -%}
            {%- if not skip.v -%}
                {%- if r[0] not in tables -%}{%- do tables.update({r[0]: {}}) -%}{%- endif -%}
                {%- do tables[r[0]].update({r[1]: r[2]}) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- for t in tables.keys() | list -%}
        {%- for rc in require_columns -%}
            {%- if rc not in tables[t] and t in tables -%}{%- do tables.pop(t) -%}{%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if tables | length == 0 %}
    select
        cast(null as varchar) as source_table,
        cast(null as bigint) as source_line
        {%- for a in aliases %},
        cast(null as varchar) as {{ a }}
        {%- endfor %}
    where false
    {%- else -%}
    {%- for t, cols in tables.items() %}
    select
        '{{ t }}' as source_table,
        cast("_line" as bigint) as source_line
        {%- for a in aliases %}
        {%- set found = namespace(c=none) -%}
        {%- for cand in columns[a] -%}{%- if found.c is none and cand in cols -%}{%- set found.c = cand -%}{%- endif -%}{%- endfor %},
        {% if found.c is none -%}
            cast(null as varchar)
        {%- elif 'WITH TIME ZONE' in cols[found.c] -%}
            nullif(cast(timezone('UTC', "{{ found.c }}") as varchar), '')
        {%- else -%}
            nullif(cast("{{ found.c }}" as varchar), '')
        {%- endif %} as {{ a }}
        {%- endfor %}
    from {{ db }}.{{ sch }}."{{ t }}"
    {%- if not loop.last %}
    union all
    {%- endif -%}
    {%- endfor -%}
    {%- endif -%}
{%- endmacro %}
