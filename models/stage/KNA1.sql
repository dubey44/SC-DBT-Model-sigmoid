{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_kna1_cdf_base') }}
