{{ config(materialized='incremental') }}

with batch_master as
(
    select * 
    from {{ref('batch_master')}}
),
inventory_movement as(
   select * 
   from {{ref('inventory_movement')}}
),
shiplife as(
   select *
   from {{ref('shiplife')}}
)

SELECT
   A.Year,--fiscal year
   A.Month,--fiscal Month
   A.date,--Date created by fiscal Year and Month
   A.MaterialId,
   A.PlantSLocID,
   A.PlantId,
   A.StorageLocationId,
   A.BatchId,
   A.BaseUnit,
   A.StockQuantity,
   A.UnitPrice,
   A.StockValue,
   A.UnrestrictedStockQuantity,
   A.UnrestrictedStockValue,
   A.StockInTransitQuantity,
   A.StockInTransitvalue,
   A.QualityInspectionQuantity,
   A.QualityInspectionValue,
   A.BlockedStockQuantity,
   A.BlockedStockValue,
   A.BatchRestrictedStockQuantity,
   A.BatchRestrictedStockValue,
   A.ReturnsStockQuantity,
   A.ReturnsStockValue,
   A.MARA_ABC_Analysis,
   A.MARC_ABC_Analysis,
   B.LastPostingDate,
   -- Inventory aging considers the end date of the fiscal period for past periods and the current date for the latest period.
   case WHEN B.LastPostingDate is null then NULL
   -- else datediff(day,B.LastPostingDate,A.Calander_Date)
   ELSE DATE_PART('day', A.Calander_Date) - DATE_PART('day', B.LastPostingDate)
   end as InventoryAging,
   C.manufacture_date,
   C.shelf_life_expiration_date,
   -- datediff(day,getdate(),C.shelf_life_expiration_date) as Shelf_Life_Remaining_Days,
   DATE_PART('day', C.shelf_life_expiration_date) - DATE_PART('day', CURRENT_DATE) AS Shelf_Life_Remaining_Days,
   C.ship_life_expiration_date,
   -- datediff(day,getdate(),C.ship_life_expiration_date) as Ship_Life_Remaining_Days
   DATE_PART('day', C.ship_life_expiration_date) - DATE_PART('day', CURRENT_DATE) AS Ship_Life_Remaining_Days
FROM
   batch_master AS A
LEFT JOIN
   inventory_movement AS B
   ON A.Date = B.Date
   AND A.MaterialId = B.MaterialId
   AND A.PlantId = B.PlantID
   AND A.StorageLocationId = B.StorageLocationId
   AND A.BatchId = B.BatchId
LEFT JOIN
   shiplife AS C
   ON A.MaterialId = C.MaterialId
   AND A.BatchId = C.BatchId
   where A.Year='2024' and A.Month IN ('10','11')
