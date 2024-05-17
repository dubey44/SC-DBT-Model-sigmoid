{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbak_curated_scd1') }}
