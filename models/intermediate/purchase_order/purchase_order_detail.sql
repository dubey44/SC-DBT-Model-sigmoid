{{ config(materialized='incremental') }}

with ekko as
(
    select purchasing_document_num
    from {{ref('EKKO')}}
),
ekpo as
(
    select plant,material_number,purchasing_document_num,purchasing_document_item_num,vendor_to_be_supplied
    from {{ref('EKPO')}}
)
select
ekko.purchasing_document_num as purchasing_doc_num,
ekpo.purchasing_document_item_num as purchasing_requisition_item_no,
ekpo.vendor_to_be_supplied as vendor_to_be_supplied,
ekpo.plant as plant,
ekpo.material_number as material_number
from ekko
join ekpo
on ekko.purchasing_document_num = ekpo.purchasing_document_num