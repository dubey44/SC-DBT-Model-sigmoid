{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_kna_curated_scd1') }}
