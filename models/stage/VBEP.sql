{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_vbep_curated_scd1') }}