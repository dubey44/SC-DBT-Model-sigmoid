{{ config(materialized='incremental') }}

with tbl_otm_location as(
    select * from {{ref('tbl_otm_location')}}
)
select * from tbl_otm_location