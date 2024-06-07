
with VBEP as(
    select 
    sales_document as VBELN,
    goods_issue_date as WADAT
    from {{ref('VBEP')}}
)
SELECT VBELN,
       MAX(WADAT) AS PLANNED_GI
FROM VBEP
GROUP BY VBELN