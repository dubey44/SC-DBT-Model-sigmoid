{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_likp_curated_scd1') }}