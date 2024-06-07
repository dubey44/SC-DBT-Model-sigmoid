{{ config(materialized='incremental') }}

with tbl_otm_shipment_refnum as(
    select * from {{ref('tbl_otm_shipment_refnum')}}
)
select * from tbl_otm_shipment_refnum