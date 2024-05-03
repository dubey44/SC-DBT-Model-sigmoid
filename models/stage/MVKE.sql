{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_mvke_cdf_base') }}
