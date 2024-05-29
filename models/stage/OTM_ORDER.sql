{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'otm_order') }}