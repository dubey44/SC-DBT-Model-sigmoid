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
   else datediff(day,B.LastPostingDate,A.Calander_Date)
   end as InventoryAging,
   C.manufacture_date,
   C.shelf_life_expiration_date,
   datediff(day,getdate(),C.shelf_life_expiration_date) as Shelf_Life_Remaining_Days,
   C.ship_life_expiration_date,
   datediff(day,getdate(),C.ship_life_expiration_date) as Ship_Life_Remaining_Days
FROM
   ${catalog_name}.consumption_inventory.tbl_dp_fact_inventory AS A
LEFT JOIN
   ${catalog_name}.consumption_inventory.tbl_dp_fact_inventorymovement AS B
   ON A.`Date` = B.`Date`
   AND A.MaterialId = B.MaterialId
   AND A.PlantId = B.PlantID
   AND A.StorageLocationId = B.StorageLocationId
   AND A.BatchId = B.BatchId
LEFT JOIN
 ${catalog_name}.consumption_inventory.tbl_dp_fact_Shiplife AS C
   ON A.MaterialId = C.MaterialId
   AND A.BatchId = C.BatchId
   where A.Year="2024" and A.Month IN ("10","11")
