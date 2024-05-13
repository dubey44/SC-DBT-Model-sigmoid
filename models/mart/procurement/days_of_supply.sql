{{ config(materialized='incremental') }}

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
select 
i.material_number as material_number,
i.storage_loc as storage_loc,
pod.vendor_to_be_supplied as supplier,
pod.plant as plant,
vm.geo_location as geo_location,
vm.industry as industry,
sum(i.volume) as volume,
sum(mu.daily_usage) as daily_usage,
sum(i.volume)/sum(mu.daily_usage) as days_of_supply,
i.material_type as sku,
i.product as product,
i.brand as brand,
sod.sales_channel as sales_channel
-- customer_master.cust_segment as cust_segment,   -- not exists in customer_master
-- sales_order_detail.order_type       -- not exists in sales_order_detail

from 
inventory i
left join sales_order_detail sod on i.material_number = sod.customer_number 
left join purchase_order_detail pod on i.material_number = pod.material_number
left join vendor_master vm on vm.vendor_account_no = pod.vendor_to_be_supplied
left join material_usage mu on mu.material_number = i.material_number and i.storage_loc = mu.storage_loc and mu.plant = i.plant

group by
i.material_number,
i.storage_loc,
pod.vendor_to_be_supplied,
pod.plant,
vm.geo_location,
vm.industry,
i.material_type,
i.product,
i.brand,
sod.sales_channel
-- cust_segment




