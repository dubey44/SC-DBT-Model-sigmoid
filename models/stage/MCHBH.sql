{{ config(materialized='table') }}

select
    *
from
    {{ source('raw_source', 'mchbh') }}