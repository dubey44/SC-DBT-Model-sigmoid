{{ config(materialized='table') }}

with ekko as
(
    select EBELN
    from {{ref('EKKO')}}
),
ekpo as
(
    select EBELN,EBELP,EMLIF,WERKS,EKPO 
    from {{ref('EKPO')}}
)
select
ekko.EBELN as purchasing_doc_num,
ekpo.EBELP as purchasing_requisition_item_no,
ekpo.EMLIF as vendor_to_be_supplied,
ekpo.WERKS as plant,
ekpo.EKPO as material_number
from ekko
join ekpo
on ekko.EBELN = ekpo.EBELN