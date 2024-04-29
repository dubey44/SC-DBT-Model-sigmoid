{{ config(materialized='table') }}

with vbak as
(
    select VBELN,VTWEG
    from {{ref('VBAK')}}
),
vbap as(
    select VBELN,POSNR,MATNR
    from {{ref('VBAP')}}
),
vbep as(
    select VBELN,BMENG,WEPOS 
    from {{ref('VBEP')}}
)
select 
vbak.VBELN as sales_document,
vbap.POSNR as sales_document_item,
vbap.MATNR as material_number,
vbep.BMENG as confirmed_quantity,
vbep.WEPOS as goods_issue_date,
vbak.VTWEG as sales_channel
from vbak 
left join vbap on vbak.VBELN = vbap.VBELN
join vbep on vbak.VBELN = vbep.VBELN
