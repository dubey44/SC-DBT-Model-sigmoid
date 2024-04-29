{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_vbak_curated_scd1') }}
