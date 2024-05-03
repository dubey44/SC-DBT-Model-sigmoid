{{ config(materialized='table') }}

with vbak as
(
    select VBELN,VTWEG
    from {{ref('VBAK')}}
),
vbap as(
    select VBELN,POSNR,MATNR,WERKS,LGORT,KMPMG,ERDAT
    from {{ref('VBAP')}}
),
vbep as(
    select sales_document as VBELN ,confirmed_quantity as BMENG, goods_issue_date as WEPOS
    from {{ref('VBEP')}}
)
select 
vbak.VBELN as sales_document,
vbap.POSNR as sales_document_item,
vbap.MATNR as material_number,
vbap.WERKS as plant,
vbap.LGORT as storage_loc,
vbep.BMENG as confirmed_quantity,
vbep.WEPOS as goods_issue_date,
vbak.VTWEG as sales_channel,
vbap.ERDAT as date_of_sales,
vbap.KMPMG as quantity,
vbap.KUNNR as customer_number
from vbak 
left join vbap on vbak.VBELN = vbap.VBELN
join vbep on vbak.VBELN = vbep.VBELN