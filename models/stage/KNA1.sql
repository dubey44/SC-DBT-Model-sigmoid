{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_kna1_cdf_base') }}
