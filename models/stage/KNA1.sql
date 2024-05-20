{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_kna1_curated_scd1') }}
