with vbfa as(
    select originating_document as vbelv,document_category as vbtyp_n, * from {{ref('VBFA')}}
),
ord_code_cte as(
    select * from {{ref('ord_code_cte')}}
),
ord_cte as(
    select * from {{ref('ord_cte')}}
),
sales_order_header as(
    select * from {{ref('sales_order_header')}}
),
delivery_header as(
    select * from {{ref('delivery_header')}}
)
select vbak.sales_document
from sales_order_header as vbak --all 
-- left join sap_pgi_cte as pgi
-- on vbak.order_no = pgi.order_no
left join delivery_header as pgi
on vbak.sales_document = pgi.order_number
left join
(
    select *
    from
    (   select oc.*,
        o.count_ord
        from ord_code_cte as oc
        left join ord_cte as o
        on oc.vbeln = o.vbeln
        where abgru = '00'
    )
    where count_ord_code = count_ord
) as vbap
on vbak.sales_document = vbap.vbeln
-- where order_type in ('KL','TA','ZCON','ZDIS')
--  where request_delivery_date > dateadd(year, -1,current_date)
-- and (sap_pgi = '1900-01-01' or sap_pgi is Null) -------pgi
and (pgi.planned_goods_movement_date = '1900-01-01' or pgi.planned_goods_movement_date is Null)
and vbap.vbeln is null
and vbak.customer_purchase_order_number not in (
        select distinct vbelv
        from vbfa
        where vbtyp_n = 'r'
        )



-- SELECT VBAK.ORDER_NO
-- FROM ALL_ORDERS_CTE AS VBAK
-- LEFT JOIN SAP_PGI_CTE AS PGI
-- ON VBAK.ORDER_NO = PGI.ORDER_NO
-- LEFT JOIN
-- (
--     SELECT *
--     FROM
--     (   SELECT OC.*,
--         O.COUNT_ORD
--         FROM ORD_CODE_CTE as OC
--         LEFT JOIN ORD_CTE as O
--         ON OC.VBELN = O.VBELN
--         WHERE ABGRU = '00'
--     )
--     WHERE COUNT_ORD_CODE = COUNT_ORD
-- ) AS VBAP
-- ON VBAK.ORDER_NO = vbap.VBELN
-- WHERE ORDER_TYPE LIKE ANY ('KL','TA','ZCON','ZDIS')
-- AND REQUEST_DELIVERY_DATE > DATEADD(YEAR, -1,CURRENT_DATE())
-- AND (SAP_PGI = '1900-01-01' or SAP_PGI IS NULL)
-- AND VBAP.VBELN IS NULL
-- AND VBAK.ORDER_NO NOT IN (
-- SELECT DISTINCT VBELV
-- FROM snowflake_prd_landzone.RAW_SAP.TBL_SAP_VBFA
-- WHERE VBTYP_N = 'R'

-- UNION

-- SELECT DISTINCT VBELV
-- FROM snowflake_prd_landzone.RAW_SAP.TBL_S4_VBFA
-- WHERE VBTYP_N = 'R'
-- )
