{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_lfa1_curated_scd1') }}
