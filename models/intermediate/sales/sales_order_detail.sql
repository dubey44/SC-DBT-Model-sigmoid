{{ config(materialized='incremental') }}

with vbak as
(
    select sales_document,distribution_channel,competitor
    from {{ref('VBAK')}}
),
vbap as(
    select sales_document,sales_document_item,material_number,plant,issue_loc_for_prod_order,component_quantity,record_created_on
    from {{ref('VBAP')}}
),
vbep as(
    select sales_document,confirmed_quantity, goods_issue_date
    from {{ref('VBEP')}}
)
select 
vbak.sales_document as sales_document,
vbap.sales_document_item as sales_document_item,
vbap.material_number as material_number,
vbap.plant as plant,
vbap.issue_loc_for_prod_order as storage_loc,
vbep.confirmed_quantity as confirmed_quantity,
vbep.goods_issue_date as goods_issue_date,
vbak.distribution_channel as sales_channel,
vbap.record_created_on as date_of_sales,
vbap.component_quantity as quantity,
vbak.competitor as customer_number
from vbak 
left join vbap on vbak.sales_document = vbap.sales_document
join vbep on vbak.sales_document = vbep.sales_document

-- {{ config(materialized='incremental') }}

-- with vbak as
-- (
--     select VBELN,VTWEG,KUNNR
--     from {{ref('VBAK')}}
-- ),
-- vbap as(
--     select VBELN,POSNR,MATNR,WERKS,LGORT,KMPMG,ERDAT
--     from {{ref('VBAP')}}
-- ),
-- vbep as(
--     select sales_document as VBELN ,confirmed_quantity as BMENG, goods_issue_date as WEPOS
--     from {{ref('VBEP')}}
-- )
-- select 
-- vbak.VBELN as sales_document,
-- vbap.POSNR as sales_document_item,
-- vbap.MATNR as material_number,
-- vbap.WERKS as plant,
-- vbap.LGORT as storage_loc,
-- vbep.BMENG as confirmed_quantity,
-- vbep.WEPOS as goods_issue_date,
-- vbak.VTWEG as sales_channel,
-- vbap.ERDAT as date_of_sales,
-- vbap.KMPMG as quantity,
-- vbak.KUNNR as customer_number
-- from vbak 
-- left join vbap on vbak.VBELN = vbap.VBELN
-- join vbep on vbak.VBELN = vbep.VBELN