{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_vbap_curated_scd1') }}
