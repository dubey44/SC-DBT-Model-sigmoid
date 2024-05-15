{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbap_curated_scd1') }}
