{{ config(materialized='incremental') }}

with VBAK as
(
    select *
    from {{ref('VBAK')}}
),
VBEP as(
    select client,sales_document,confirmed_quantity, goods_issue_date
    from {{ref('VBEP')}}
),
VBUK as(
    select * from 
    {{ref('VBUK')}}
)
SELECT 
VBAK.client AS client, 
VBAK.sales_document AS sales_document, 
VBAK.sales_organization AS sales_organization,
VBAK.customer_purchase_order_number AS customer_purchase_order_number,
VBAK.delivery_block AS delivery_block, 
VBAK.billing_block AS billing_block,
VBAK.competitor AS customer_id,
VBAK.shipping_conditions AS shipping_conditions, 
VBAK.sales_document_type AS sales_document_type,
VBAK.record_created_on AS record_created_on,
'R3' as source,
current_date as load_date, 
sum(VBAK.net_value_order_item) AS net_value_order_item, 
max(VBAK.requested_delivery_date) AS requested_delivery_date, 
sum(VBAK.total_quantity) AS total_quantity,
max(VBAK.original_rdd) as original_rdd,
max(VBEP.goods_issue_date) AS goods_issue_date,
VBUK.overall_rejection_status_of_all_document_items as rejection_status
FROM VBAK
LEFT JOIN VBEP
    ON VBAK.sales_document = VBEP.sales_document and VBAK.client = VBEP.client
LEFT JOIN VBUK
    ON VBAK.sales_document=VBUK.Sales_and_Distribution_Document_Number and VBAK.client=VBUK.client

GROUP BY 
    VBAK.client, 
    VBAK.sales_document, 
    VBAK.sales_organization, 
    VBAK.customer_purchase_order_number, 
    VBAK.delivery_block, 
    VBAK.billing_block, 
    VBAK.competitor, 
    VBAK.shipping_conditions,
    VBAK.sales_document_type,
    VBAK.record_created_on,  
    source,
    load_date,
    rejection_status

union all 

select

VBAK.client AS client, 
VBAK.sales_document AS sales_document, 
VBAK.sales_organization AS sales_organization,
VBAK.customer_purchase_order_number AS customer_purchase_order_number,
VBAK.delivery_block AS delivery_block, 
VBAK.billing_block AS billing_block,
VBAK.competitor AS customer_id,
VBAK.shipping_conditions AS shipping_conditions, 
VBAK.sales_document_type AS sales_document_type,
VBAK.record_created_on AS record_created_on,
'S4' as source,
current_date as load_date, 
sum(VBAK.net_value_order_item) AS net_value_order_item,  
max(VBAK.requested_delivery_date) AS requested_delivery_date,
sum(VBAK.total_quantity) AS total_quantity,
max(VBAK.original_rdd) as original_rdd,
max(VBEP.goods_issue_date) AS goods_issue_date
VBUK.overall_rejection_status_of_all_document_items as rejection_status

FROM VBAK
LEFT JOIN VBEP
    ON VBAK.sales_document = VBEP.sales_document and VBAK.client = VBEP.client


GROUP BY 
    VBAK.client, 
    VBAK.sales_document, 
    VBAK.sales_organization, 
    VBAK.customer_purchase_order_number, 
    VBAK.delivery_block, 
    VBAK.billing_block, 
    VBAK.competitor, 
    VBAK.shipping_conditions, 
    VBAK.sales_document_type,
    VBAK.record_created_on,  
    source,
    load_date,
    rejection_status