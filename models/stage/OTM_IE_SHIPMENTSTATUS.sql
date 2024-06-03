{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_otm_ie_shipmentstatus') }}