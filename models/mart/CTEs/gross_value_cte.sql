with VBAP as(
    select 
    sales_document as VBELN,
    reason_for_rejection as ABGRU,
    subtotal_1_pricing as KZWI1 
    from {{ref('VBAP')}}
)
SELECT VBELN, /*Order Number*/
ABGRU, /*Line Status*/
SUM(KZWI1) AS GROSS_ORDER_VALUE
FROM VBAP
GROUP BY VBELN,ABGRU
-- HAVING ABGRU = ''
HAVING ABGRU is NULL