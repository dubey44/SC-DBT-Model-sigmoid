with VBAP as(
    select sales_document as VBELN from {{ref('VBAP')}}
)

SELECT VBELN,
COUNT(*) as COUNT_ORD
FROM VBAP
GROUP BY VBELN