with VBAP as(
    select sales_document as VBELN,
    reason_for_rejection as ABGRU,
     * from {{ref('VBAP')}}
)
SELECT VBELN,
ABGRU,
COUNT(*) as COUNT_ORD_CODE
FROM VBAP
GROUP BY VBELN, ABGRU