{{ config(materialized='incremental') }}

with mara as
(
    select material_number,volume,brand,material_type 
    from {{ref('MARA')}}
),
marc as(
    select material_number,plant,external_procurement_storage_loc, total_replenishment_lead_time
    from {{ref('MARC')}}
),
mvke as(
    select MATNR,PRODH 
    from {{ref('MVKE')}}
)
select 
mara.material_number as material_number,
marc.external_procurement_storage_loc as storage_loc, 
marc.plant as plant,
mara.volume as volume,
mara.brand as brand,
mara.material_type as material_type,
mvke.PRODH as product,
marc.total_replenishment_lead_time as total_replenishment_lead_time
from mara
join marc on mara.material_number = marc.material_number
join mvke on mara.material_number = mvke.MATNR

