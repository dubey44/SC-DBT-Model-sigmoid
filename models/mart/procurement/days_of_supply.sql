{{ config(materialized='table') }}

with inventory as 
(
    select * from {{ref('inventory')}}
),
purchase_order_detail as(
    select * from {{ref('purchase_order_detail')}}
),
vendor_master as(
    select * from {{ref('vendor_master')}}
),
material_usage as(
    select * from {{ref('material_usage')}}
),
customer_master as(
    select * from {{ref('customer_master')}}
),
sales_order_detail as(
    select * from {{ref('sales_order_detail')}}
)

