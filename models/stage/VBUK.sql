{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbuk_curated_scd1') }}