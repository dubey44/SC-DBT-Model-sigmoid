{{ config(materialized='table') }}

with ekko as
(
    select EBELN,EBELP,EMLIF,WERKS 
    from {{ref('EKKO')}}
)
select
EBELN as purchasing_doc_num,
EBELP as purchasing_requisition_item_no,
EMLIF as vendor_to_be_supplied,
WERKS as plant
from ekko