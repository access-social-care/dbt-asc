{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_locality() -> AVA.PUBLIC.ACCESSAVA_LOCALITY. Stands
  alone (not joined into accessava). One row per transcript_id from
  actions_locality: R reads SELECT * and keeps slice(1) per transcript_id, i.e.
  the first row in Postgres physical order; the Fivetran ctid_fivetran_id
  '(page,tuple)' carries that order. id is dropped, local_authority_name is
  renamed la_name. Fivetran soft deletes are dropped first.
*/

select
    la_name,
    local_authority_code,
    ward,
    ward_code,
    constituency_name,
    constituency_code,
    mso_area_name,
    mso_area_code,
    lso_area_name,
    lso_area_code,
    nhs_icb_name,
    nhs_icb_code,
    urban_rural_classification_name,
    urban_rural_classification_code,
    urban_conurbation_classification_name,
    urban_conurbation_classification_code,
    transcript_id,
    county,
    county_code
from (
    select
        local_authority_name as la_name,
        *
    from {{ source('chatbot_postgres', 'actions_locality') }}
    where not coalesce(_fivetran_deleted, false)
    qualify row_number() over (
        partition by transcript_id
        order by cast(split_part(replace(replace(ctid_fivetran_id, '(', ''), ')', ''), ',', 1) as bigint),
                 cast(split_part(replace(replace(ctid_fivetran_id, '(', ''), ')', ''), ',', 2) as bigint)) = 1
) l
