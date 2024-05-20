{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_ekpo_curated_scd1') }}