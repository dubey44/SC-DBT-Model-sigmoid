{{ config(materialized='table') }}

with inventory as 
(
    select  from {{ref('inventory')}}
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
select 
inventory.material_number as material_number,
inventory.storage_location as storage_loc,
purchase_order_detail.vendor_to_be_supplied as supplier,
purchase_order_detail.plant as plant,
vendor_master.geo_location as geo_location,
vendor_master.industry as industry,
sum(inventory.volume) as volume,
sum(material_usage.daily_usage) as daily_usage,
inventory.material_type as sku,
inventory.product as product,
inventory.brand as brand,
sales_order_detail.sales_channel as sales_channel
-- customer_master.cust_segment as cust_segment,   -- not exists in customer_master
-- sales_order_detail.order_type       -- not exists in sales_order_detail

from 
inventory i
-- left join customer_master cm on cm.cust
left join sales_order_detail sod on sod.customer_number = i.material_number
left join purchase_order_detail pod on i.material_number = pod.material_number
left join vendor_master vm on vm.vendor_account_no = pod.vendor_to_be_supplied
left join material_usage mu on mu.material_number = i.material_number and i.storage_loc = mu.storage_loc and mu.plant = i.plant




group by
material_number,
storage_location,
supplier,
plant,
geo_location,
industry,
sku,
product,
brand,
sales_channel
-- cust_segment




