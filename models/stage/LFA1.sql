{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_lfa1_curated_scd1') }}
