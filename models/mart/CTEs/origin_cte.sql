with vbap as(
    select 
        sales_document as vbeln,
        plant as werks,
        record_created_on as erdat

    from {{ref('VBAP')}}
),
vbak as(
    select 
    sales_document as vbeln

    from {{ref('VBAK')}}
)

select *
from (
    select 
    distinct vbak.vbeln,
    vbap.werks as sap_origin_id,
    row_number() over(partition by vbak.vbeln order by vbap.erdat desc) as rn
    from vbak
    left join vbap
    on vbak.vbeln = vbap.vbeln
    where vbap.werks <> 'CPCA' and vbap.werks <> 'CORP')
where rn = 1
