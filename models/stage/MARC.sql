{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_marc_curated_scd1') }}
