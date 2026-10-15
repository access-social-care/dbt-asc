{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_la(tr_ev, get_new = TRUE). One row per transcript
  with a local authority, as the CASSR name (cassr_converter), else NULL when
  the name has no CASSR mapping (R keeps those rows).

  Steps, in R order:
    1. candidates: (a) the text just before the bot message "Thanks for that
       $UserName. We believe you're based in $LocalAuthority, is this
       correct?" (not 'back'); (b) text of events whose raw_record mentions
       local_authority_name. (b) wins over (a) where both exist.
    2. lower-case; names containing a digit are postcodes: upper-case, strip
       spaces, look up actions_localauthority.postcode_lookup; use the county
       when not blank, else local_authority_name; no hit -> 'error'.
       Drop 'next', 'error', '%pseudo%'; 'gl%' -> 'gloucestershire'.
    3. capwords (first letter of each word upper-case), keep the first
       candidate per transcript in event order.
    4. no LA given: infer from the tenant (seed chatbot_tenant_la_map).
    5. cassr_converter: lower-case, map through seed chatbot_la_to_cassr.
  Output la_name is the CASSR name.
*/

with ev as (
    select
        transcript_id, id, text, raw_record, tenant_name,
        lead(text, 1) over (partition by transcript_id order by id) as next_text
    from {{ ref('stg_chatbot_transcript_event') }}
),

a as (
    select transcript_id, text as la_text, min(id) as pos
    from ev
    where next_text = 'Thanks for that $UserName. We believe you''re based in $LocalAuthority, is this correct?'
      and text <> 'back'
    group by transcript_id, text
),

b as (
    select transcript_id, text as la_text, min(id) as pos
    from ev
    where contains(raw_record, 'local_authority_name')
    group by transcript_id, text
),

joined as (
    select
        coalesce(b.transcript_id, a.transcript_id) as transcript_id,
        coalesce(b.la_text, a.la_text) as la_text,
        coalesce(b.pos, a.pos) as pos,
        a.pos as pos2
    from b full join a on b.transcript_id = a.transcript_id
),

lowered as (
    select transcript_id, lower(la_text) as la_text, min(pos * 1000000 + coalesce(pos2, 0)) as pos
    from joined
    where la_text is not null
    group by transcript_id, lower(la_text)
),

postcodes as (
    select
        upper(replace(postcode_lookup, ' ', '')) as clean,
        local_authority_name,
        local_authority_county
    from {{ source('chatbot_postgres', 'actions_localauthority') }}
    where not coalesce(_fivetran_deleted, false)
),

mapped as (
    select
        l.transcript_id,
        l.pos,
        case
            when regexp_matches(l.la_text, '[0-9]') then
                coalesce(
                    case when p.local_authority_county = '' then p.local_authority_name else p.local_authority_county end,
                    'error')
            else l.la_text
        end as la_text
    from lowered l
    left join postcodes p
        on regexp_matches(l.la_text, '[0-9]') and upper(replace(l.la_text, ' ', '')) = p.clean
),

cleaned as (
    select
        transcript_id,
        pos,
        case when la_text = 'gl%' then 'gloucestershire' else la_text end as la_text
    from mapped
    where la_text not in ('next', 'error') and la_text not like '%pseudo%'
),

typed as (
    select
        transcript_id,
        array_to_string(list_transform(string_split(la_text, ' '), w -> upper(w[1:1]) || w[2:]), ' ') as la_name
    from cleaned
    qualify row_number() over (partition by transcript_id order by pos) = 1
),

inferred as (
    select distinct e.transcript_id, m.la_name as inferred_la
    from ev e
    join {{ ref('chatbot_tenant_la_map') }} m on e.tenant_name = m.tenant_name
),

resolved as (
    select distinct
        coalesce(t.transcript_id, i.transcript_id) as transcript_id,
        coalesce(t.la_name, i.inferred_la) as la_name
    from typed t
    full join inferred i on t.transcript_id = i.transcript_id
    where coalesce(t.la_name, i.inferred_la) is not null
)

select
    r.transcript_id,
    c.cassr_name as la_name
from resolved r
left join {{ ref('chatbot_la_to_cassr') }} c on lower(r.la_name) = c.la_name
