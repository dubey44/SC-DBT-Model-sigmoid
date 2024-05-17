{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_ekpo_cdf_base') }}