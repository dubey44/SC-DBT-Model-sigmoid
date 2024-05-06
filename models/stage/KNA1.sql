{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_kna1_cdf_base') }}
