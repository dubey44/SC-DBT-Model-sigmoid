{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbrp_curated_scd1') }}

