{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'tbl_mvke_cdf_base') }}
