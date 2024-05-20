{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_ekko_curated_scd1') }}