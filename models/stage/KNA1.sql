{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'kna1') }}
