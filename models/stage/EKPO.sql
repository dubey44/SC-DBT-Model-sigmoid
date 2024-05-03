{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'tbl_ekpo_cdf_base') }}