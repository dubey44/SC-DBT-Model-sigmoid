{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_vbep_cdf_base') }}
    

-- tbl_vbep_curated_scd1
-- tbl_vbep_cdf_base
