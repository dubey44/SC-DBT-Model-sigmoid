{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbak_curated_scd1') }}
