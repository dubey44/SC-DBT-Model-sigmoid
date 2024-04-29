{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_mara_curated_scd1') }}
