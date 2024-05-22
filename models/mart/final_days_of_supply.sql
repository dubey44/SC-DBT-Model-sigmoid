{{ config(materialized='incremental') }}

with inventory as 
(
    select * from {{ref('final_inventory')}}
),
sales_order_detail as 
(
    select * from {{ref('sales_order_detail')}}
),
purchase_order_detail as 
(
    select * from {{ref('purchase_order_detail')}}
),
material_usage as
(
    select * from {{ref('material_usage')}}
)

select 
i.materialid as material_number,
i.plantslocid as storage_loc,
pod.vendor_to_be_supplied as supplier,
pod.plant as plant,
sum(i.stockquantity) as volume,
sum(mu.daily_usage) as daily_usage,
sum(i.stockquantity)/sum(mu.daily_usage) as days_of_supply,
sum(i.unrestrictedstockquantity) as unrestrictedstockquantity,
sum(i.unrestrictedstockvalue) as unrestrictedstockvalue,

sod.sales_channel as sales_channel

from 
inventory i
left join sales_order_detail sod ON i.materialid = sod.material_number
left join purchase_order_detail pod ON i.materialid = pod.material_number
left join material_usage mu ON mu.material_number = i.materialid

group by
i.materialid,
i.plantslocid,
pod.vendor_to_be_supplied,
pod.plant,
sod.sales_channel