{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'tbl_otm_ss_status_history') }}