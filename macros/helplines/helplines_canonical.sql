{#
  Shared pieces of the partner reader rules (partner_readers.R on helplines_data main).
  Each macro renders SQL text; models stay readable and every partner applies
  the same sentinel, mapping and version rules.
#}

{# Missing T1/T2 become the sentinel "999" (R: is.na(x) | x == "NA"). #}
{% macro helplines_sentinel(col) -%}
    case when {{ col }} is null or {{ col }} = 'NA' then '999' else {{ col }} end
{%- endmacro %}

{# Org rows of the universal themes map, as R's UC_map (unique over org, T1, T2, UT1, UT2).
   Keys are joined as stored: R lower-cases the partner side only, never the map. #}
{% macro helplines_uc_map(org) -%}
    (select distinct t1, t2, ut1, nullif(ut2, 'NA') as ut2
     from {{ source('reference', 'universal_themes_map') }}
     where org = '{{ org }}')
{%- endmacro %}

{# One source file/sheet per month. Fivetran tables carry no load order, so "first-seen
   month wins" (the monthly readers' rule: diff against history, never revise a month)
   is approximated from content. `rule`:
     fullest     the version with most rows (weight) for the month; ties go to the earlier
                 source_table name. Mencap/RNIB's own R rule (reconcile_annual_months).
     first_seen  the version whose file ends earliest, i.e. the file that was delivered
                 first and so would have been read first. Approximation of history order.
     latest      the version whose file ends latest (newest delivery supersedes).
   `rel` is a CTE name with month, source_table and date columns. #}
{% macro helplines_pick_version(rel, rule='fullest', month_col='month', version_col='source_table', date_col='date', weight='1') -%}
    (select r.*
     from {{ rel }} r
     inner join (
         select m, v
         from (
             select {{ month_col }} as m, {{ version_col }} as v, sum({{ weight }}) as n,
                    max(max({{ date_col }})) over (partition by {{ version_col }}) as vmax
             from {{ rel }}
             group by 1, 2
         )
         qualify row_number() over (
             partition by m
             order by
             {%- if rule == 'fullest' %} n desc, v asc
             {%- elif rule == 'first_seen' %} vmax asc, v asc
             {%- elif rule == 'latest' %} vmax desc, v desc
             {%- else %} {{ exceptions.raise_compiler_error('helplines_pick_version: unknown rule ' ~ rule) }}
             {%- endif %}
         ) = 1
     ) w
       on r.{{ month_col }} is not distinct from w.m
      and r.{{ version_col }} = w.v)
{%- endmacro %}

{# year_month(): YYYY-MM #}
{% macro helplines_year_month(date_col) -%}
    strftime({{ date_col }}, '%Y-%m')
{%- endmacro %}

{# cassr_converter(): lower-case the LA name, look up the CASSR name. NULL if absent.
   Pass a QUALIFIED column (alias.la_name): the subquery has its own la_name column, and an
   unqualified la_name binds to it instead of the outer row, silently returning an arbitrary CASSR. #}
{% macro helplines_cassr_name(la_name_col) -%}
    (select c.cassr_name from {{ source('reference_la', 'la_to_cassr') }} c where c.la_name = lower({{ la_name_col }}) limit 1)
{%- endmacro %}

{# reparse norm_t(): trim, lower-case, blank or 'na' -> '999'. #}
{% macro helplines_norm_t(col) -%}
    case when {{ col }} is null or trim(lower({{ col }})) in ('', 'na') then '999' else trim(lower({{ col }})) end
{%- endmacro %}

{# reparse norm_t_loose(): norm_t, then curly quotes/dashes/underscores/ampersands and spacing flattened. #}
{% macro helplines_norm_t_loose(col) -%}
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        replace(replace(regexp_replace(regexp_replace(regexp_replace({{ helplines_norm_t(col) }}, '[‘’‛`]', '''', 'g'), '[“”]', '"', 'g'), '[–—−]', '-', 'g'), '_', ' '), '&', ' and '),
        '\s*-\s*', ' - ', 'g'), '\s*/\s*', '/', 'g'), '[.,;:!?]+$', '', 'g'), '\s+', ' ', 'g'), '^\s+|\s+$', '', 'g'), '\s+', ' ', 'g'))
{%- endmacro %}
