{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_mara_curated_scd1') }}
