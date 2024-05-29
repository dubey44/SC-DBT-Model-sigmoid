{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_lips_curated_scd1') }}