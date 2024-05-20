{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'mvke') }}
