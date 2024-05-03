{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_marc_curated_scd1') }}
