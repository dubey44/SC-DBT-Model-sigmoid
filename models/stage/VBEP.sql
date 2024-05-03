{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_vbep_curated_scd1') }}