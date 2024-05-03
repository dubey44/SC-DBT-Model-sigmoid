{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_vbrp_curated_scd1') }}

