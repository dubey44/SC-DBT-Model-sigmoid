{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbrp_curated_scd1') }}

