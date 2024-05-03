{{ config(materialized='table') }}

with sales_order_detail as
(
    select material_number, plant, storage_loc, confirmed_quantity
    from {{ref('sales_order_detail')}}
)
select material_number, plant, storage_loc, sum(confirmed_quantity)/365 as daily_usage 
from sales_order_detail
group by material_number,plant,storage_loc