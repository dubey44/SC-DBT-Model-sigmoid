with VBAP as(
    select * from {{ref('VBAP')}}
)

SELECT sales_document as VBELN,
COUNT(*) as COUNT_ORD
FROM VBAP
GROUP BY VBELN