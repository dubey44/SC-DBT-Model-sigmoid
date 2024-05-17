-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_name", " ")
-- MAGIC catalog = dbutils.widgets.get("catalog_name")
-- MAGIC spark.conf.set("catalog_name", catalog)

-- COMMAND ----------

-- takes an input, return the same if not null else 0 
CREATE OR REPLACE FUNCTION ${catalog_name}.curated_inventory.get_num(num INT)
  RETURNS INT
  RETURN coalesce(num, 0);

-- COMMAND ----------

-- takes two inputs, return division of it 
CREATE OR REPLACE FUNCTION ${catalog_name}.curated_inventory.get_division(num1 INT, num2 INT)
  RETURNS INT
  RETURN coalesce(num1 / num2,0);

-- COMMAND ----------

-- takes five inputs, return sum of it 
CREATE OR REPLACE FUNCTION ${catalog_name}.curated_inventory.get_sum(num1 INT, num2 INT, num3 INT, num4 INT, num5 INT)
  RETURNS INT
  RETURN coalesce(num1 + num2 + num3 + num4 + num5, 0);

-- COMMAND ----------

-- takes two inputs(year and month) and returns last day of the given month and year 
CREATE OR REPLACE FUNCTION ${catalog_name}.curated_inventory.get_last_day(year STRING, month STRING)
  RETURNS STRING
  RETURN last_day(CONCAT(year, '-', month, '-', '1'))

-- COMMAND ----------

-- DDL for table creation
--Contains the information about Quantity and Valuation of Stocks(Stock_On_Hand,UnrestrictedStock,StockInTransit,QualityInspection,BlockedStock,RestrictedStock,ReturnStock) along with the ABC Analysis from MARA and MARC
CREATE TABLE IF NOT EXISTS ${catalog_name}.curated_inventory.tbl_inventory_fact (
  year INT,
  month INT,
  date DATE,
  calander_date DATE,
  material_id STRING,
  plant_storage_loc_id STRING,
  plant_id STRING,
  storage_loc_id STRING,
  batch_id STRING,
  base_unit STRING,
  stock_qty DECIMAL(20, 3),
  unit_price DECIMAL(20, 3),
  stock_value DECIMAL(20, 3),
  unrestricted_stock_qty DECIMAL(20, 3),
  unrestricted_stock_value DECIMAL(20, 3),
  stock_transit_qty DECIMAL(20, 3),
  stock_transit_value DECIMAL(20, 3),
  quality_inspection_qty DECIMAL(20, 3),
  quality_inspection_value DECIMAL(20, 3),
  blocked_stock_qty DECIMAL(20, 3),
  blocked_stock_value DECIMAL(20, 3),
  batch_restricted_stock_qty DECIMAL(20, 3),
  batch_restricted_stock_value DECIMAL(20, 3),
  returns_stock_qty DECIMAL(20, 3),
  returns_stock_value DECIMAL(20, 3),
  mara_abc_analysis STRING,
  marc_abc_analysis STRING
)
USING delta

-- COMMAND ----------

-- MCHBH view 
CREATE OR REPLACE TEMPORARY VIEW MCHBH AS 
  SELECT *,${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date,
      coalesce(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period))
            OVER(PARTITION BY plant,storage_location,material_num 
            ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_mchbh_curated_scd1 

-- COMMAND ----------

-- MBEWH view
CREATE OR REPLACE TEMPORARY VIEW MBEWH AS 
  select *,${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date, 
      coalesce(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period))
            OVER(PARTITION BY valuation_area,material_number
            ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_mbewh_curated_scd1 WHERE valuation_type = " "

-- COMMAND ----------

-- MARC view
CREATE OR REPLACE TEMPORARY VIEW MARC AS 
select * from ${catalog_name}.curated_sap.tbl_marc_curated_scd2 where `__END_AT` is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MARA view
CREATE OR REPLACE TEMPORARY VIEW MARA AS 
select * from ${catalog_name}.curated_sap.tbl_mara_curated_scd2 where `__END_AT` is null

-- COMMAND ----------

-- MBEW view
CREATE OR REPLACE TEMPORARY VIEW MBEW AS 
select * FROM ${catalog_name}.curated_sap.tbl_mbew_curated_scd1 where valuation_type = " "

-- COMMAND ----------

-- MKOLH view
CREATE OR REPLACE TEMPORARY VIEW MKOLH AS 
  SELECT *,${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date,
      coalesce(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period))
            OVER(PARTITION BY plant,storage_location,material_num 
            ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_mkolh_curated_scd1

-- COMMAND ----------

-- MSKAH view
CREATE OR REPLACE TEMPORARY VIEW MSKAH AS 
  SELECT *,${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date,
      coalesce(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period))
            OVER(PARTITION BY plant,storage_location,material_num 
            ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_mskah_curated_scd1 

-- COMMAND ----------

-- MARCH view
CREATE OR REPLACE TEMPORARY VIEW MARCH AS 
  SELECT *,${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date,
      coalesce(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period))
            OVER(PARTITION BY plant,material_number 
            ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_march_curated_scd1 

-- COMMAND ----------

-- MSLBH view
CREATE OR REPLACE TEMPORARY VIEW MSLBH AS 
  SELECT *,${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date,
      coalesce(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period))
            OVER(PARTITION BY plant,material_num 
            ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_mslbh_curated_scd1 

-- COMMAND ----------

-- LIPS view
CREATE OR REPLACE TEMPORARY VIEW LIPS AS 
  select * from ${catalog_name}.curated_sap.tbl_lips_curated_scd1 where document_item_category="KRN" and special_stock_ind="W" and movement_type_inv_mgmt="634"

-- COMMAND ----------

----------------------------------------  LOGIC FOR MBEW AND MBEWH------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW batch_master AS   
  SELECT
  YEAR(Date.Date) AS Year,--fiscal year
  Month(Date.Date) AS Month,--fiscal month
  Date.Date AS Date,--Date created by fiscal year and fiscal month 
  date.Calander_Date,--last calander date for the fiscal month
  MCHB.material_number AS MaterialId,-- Material ID
  concat(MCHB.plant,MCHB.issue_loc_for_prod_order) AS PlantSLocID,-- Concatenating Plant and Storage Location IDs
  MCHB.plant AS PlantId,-- Plant ID
  MCHB.issue_loc_for_prod_order AS StorageLocationId,-- Storage Location ID
  MCHB.batch_number AS BatchId,-- Batch ID
  MARA.base_uom AS BaseUnit,-- Base Unit of Measurement
  --Stock On Hand

  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the StockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period)
    THEN ${catalog_name}.curated_inventory.get_sum(MCHB.valuated_unrestricted_use_stock,  MCHB.stock_in_qual_inspection, MCHB.restricted_batches_total_stock, MCHB.blocked_stock, MCHB.blocked_stock_return)
    ELSE ${catalog_name}.curated_inventory.get_sum(MCHBH.CLABS, MCHBH.CINSM, MCHBH.CEINM, MCHBH.CSPEM, MCHBH.CRETM)
    END AS StockQuantity,

  --Unit Price Of a Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the Unit Price Of a Stock calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END AS UnitPrice,

    --Stock On Hand Value
    /* The valuation of stock on hand involves multiplying the stock quantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period)
    THEN ${catalog_name}.curated_inventory.get_sum(MCHB.valuated_unrestricted_use_stock,  MCHB.stock_in_qual_inspection, MCHB.restricted_batches_total_stock, MCHB.blocked_stock, MCHB.blocked_stock_return) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_sum(MCHBH.CLABS, MCHBH.CINSM, MCHBH.CEINM, MCHBH.CSPEM, MCHBH.CRETM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS StockValue,

  --Unrestricted Stock 
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.valuated_unrestricted_use_stock)
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CLABS)
    END AS UnrestrictedStockQuantity,

    /* The valuation of UnrestrictedStock involves multiplying the UnrestrictedStockQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.valuated_unrestricted_use_stock) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CLABS) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS UnrestrictedStockValue,

    -- stock in Transit
    0 StockInTransitQuantity,
    0 StockInTransitvalue,

  --quality Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the QualityInspectionQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.stock_in_qual_inspection)
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CINSM)
    END AS QualityInspectionQuantity,
    
    /* The valuation of QualityInspection stock involves multiplying the QualityInspectionQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period)  
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.stock_in_qual_inspection) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CINSM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS QualityInspectionValue,

    --Blocked Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BlockedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period)  
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.blocked_stock)
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CSPEM)
    END AS BlockedStockQuantity,

    /* The valuation of BlockedStock involves multiplying the BlockedStockQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.blocked_stock) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CSPEM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BlockedStockValue,

    --Restricted Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BatchRestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.restricted_batches_total_stock)
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CEINM)
    END AS BatchRestrictedStockQuantity,

    /* The valuation of BatchRestrictedStock involves multiplying the BatchRestrictedStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period)  
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.restricted_batches_total_stock) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CEINM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BatchRestrictedStockValue,

    --Return Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the ReturnsStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.blocked_stock_return)
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CRETM)
    END AS ReturnsStockQuantity,

    /* The valuation of ReturnStock involves multiplying the ReturnsStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MCHB.current_period_fiscal_year, MCHB.current_period)  
    THEN ${catalog_name}.curated_inventory.get_num(MCHB.blocked_stock_return) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MCHBH.CRETM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS ReturnsStockValue,
    MARA.summary_indicator as MARA_ABC_Analysis,
    MARC.abc_indicator as MARC_ABC_Analysis
  FROM ${catalog_name}.curated_sap.tbl_mchb_curated_scd1 AS MCHB
  left join MBEW
  on MCHB.material_number = MBEW.material_number and MCHB.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON MCHB.material_number = MARA.material_number
  left join MARC
  on MCHB.material_number = MARC.material_number and MCHB.plant = MARC.plant
  /* A cross join is performed on the time dimension, focusing on dates from past 3 years up to the present. 
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month. 
    The last calendar date is selected for each fiscal month. */
  CROSS JOIN (select to_date(max(`day`)) as Calander_Date, ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from ${catalog_name}.consumption_masterdata.vw_dp_md_time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<getdate() group by ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date
  /* When sourcing data from the historical table, we select the closest previous entry to the fiscal period. */
  LEFT JOIN  MCHBH
  ON MCHB.material_number = MCHBH.material_num AND MCHB.plant = MCHBH.plant AND MCHB.issue_loc_for_prod_order = MCHBH.storage_location AND MCHB.batch_number = MCHBH.batch_num AND (MCHBH.date = Date.Date OR (Date.Date > MCHBH.Date AND Date.DATE < MCHBH.NextDate))
  /* When sourcing data from the historical table, we select the closest previous entry to the fiscal period. */
  LEFT JOIN  MBEWH
  ON MBEWH.material_number = MCHB.material_number AND MCHB.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate)) 


-- COMMAND ----------

------------------------------------------------LOGIC FOR MKOL AND MKOLH---------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW special_stocks_from_vendor AS 
  SELECT
  YEAR(Date.Date) AS Year, --fiscal year
  Month(Date.Date) AS Month, --fiscal year
  Date.Date AS Date, --Date created by fiscal year and fiscal month
  date.Calander_Date, --last calander date for the fiscal month
  MKOL.material_num AS MaterialId, -- Material ID
  Concat(MKOL.plant,MKOL.storage_location) AS PlantSLocID, -- Concatenating Plant and Storage Location IDs
  MKOL.plant AS PlantId, -- Plant ID
  MKOL.storage_location AS StorageLocationId, -- Storage Location ID
  MKOL.batch_num AS BatchId, -- Batch ID
  MARA.base_uom AS BaseUnit, -- Base Unit of Measurement

  /* A cross join is performed on the time dimension, focusing on dates from 2020 onwards up to the present. 
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month. 
    The last calendar date is selected for each fiscal month. */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON)
    THEN ${catalog_name}.curated_inventory.get_sum(MKOL.SLABS, MKOL.SINSM, MKOL.SEINM, MKOL.SSPEM, 0)
    ELSE ${catalog_name}.curated_inventory.get_sum(MKOLH.SLABS, MKOLH.SINSM, MKOLH.SEINM, MKOLH.SSPEM, 0)
    END AS StockQuantity,

  --Unit Price Of a Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the Unit Price Of a Stock calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END AS UnitPrice,
    /* The valuation of ReturnsStock involves multiplying the ReturnsStockQuantity by the unit price. */
    -- TOTAL STOCK
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON)
    THEN ${catalog_name}.curated_inventory.get_sum(MKOL.SLABS, MKOL.SINSM, MKOL.SEINM, MKOL.SSPEM, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_sum(MKOLH.SLABS, MKOLH.SINSM, MKOLH.SEINM, MKOLH.SSPEM, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS StockValue,

  --unrestricted Stock 
  --Unrestricted Stock 
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON)
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SLABS)
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SLABS)
    END AS UnrestrictedStockQuantity,

  /* The valuation of UnrestrictedStock involves multiplying the UnrestrictedStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SLABS) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SLABS) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS UnrestrictedStockValue,

    -- stock in Transit
    0 StockInTransitQuantity,
    0 StockInTransitvalue,

  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the QualityInspectionQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  --quality Stock
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SINSM)
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SINSM)
    END AS QualityInspectionQuantity,

  /* The valuation of QualityInspection stock involves multiplying the QualityInspectionQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON)
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SINSM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SINSM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS QualityInspectionValue,

    --Blocked Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BlockedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SSPEM)
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SSPEM)
    END AS BlockedStockQuantity,
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SSPEM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SSPEM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BlockedStockValue,

    --Restricted Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BatchRestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SEINM)
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SEINM)
    END AS BatchRestrictedStockQuantity,
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MKOL.LFGJA, MKOL.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MKOL.SEINM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MKOLH.SEINM) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BatchRestrictedStockValue,

    --Return Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the ReturnsStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  0 AS ReturnsStockQuantity,
  0 AS ReturnsStockValue,
    MARA.summary_indicator as MARA_ABC_Analysis,
    MARC.abc_indicator as MARC_ABC_Analysis
  FROM ${catalog_name}.curated_sap.tbl_MKOL_curated_scd1 AS MKOL
  left join MBEW
  on MKOL.material_num = MBEW.material_number and MKOL.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON MKOL.material_num = MARA.material_number
  left join MARC
  on MKOL.material_num = MARC.material_number and MKOL.plant = MARC.plant

  /* A cross join is performed on the time dimension, focusing on dates from past 3 years up to the present. 
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month. 
    The last calendar date is selected for each fiscal month. */
  CROSS JOIN (select to_date(max(`day`)) as Calander_Date, ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from ${catalog_name}.consumption_masterdata.vw_dp_md_time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<getdate() group by ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date

  LEFT JOIN MKOLH
  ON MKOL.material_num = MKOLH.material_num AND MKOL.plant = MKOLH.plant AND MKOL.storage_location = MKOLH.storage_location AND MKOL.batch_num = MKOLH.batch_num AND MKOL.special_stock_ind = MKOLH.special_stock_ind AND MKOL.vendor_account_num = MKOLH.vendor_account_num AND (MKOLH.date = Date.Date OR (Date.Date > MKOLH.Date AND Date.DATE < MKOLH.NextDate))

  LEFT JOIN MBEWH
  ON MBEWH.material_number = MKOL.material_num AND MKOL.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate)) 

-- COMMAND ----------

------------------------------------------------LOGIC FOR MSKA AND MSKAH--------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW sales_order_stock AS 
  SELECT
  YEAR(Date.Date) AS Year, --fiscal year
  Month(Date.Date) AS Month, --fiscal year
  Date.Date AS Date, --Date created by fiscal year and fiscal month
  date.Calander_Date, --last calander date for the fiscal month
  MSKA.material_num AS MaterialId, -- Material ID
  Concat(MSKA.plant,MSKA.storage_location) AS PlantSLocID, -- Concatenating Plant and Storage Location IDs
  MSKA.plant AS PlantId, -- Plant ID
  MSKA.storage_location AS StorageLocationId, -- Storage Location ID
  MSKA.batch_num AS BatchId, -- Batch ID
  MARA.base_uom AS BaseUnit, -- Base Unit of Measurement

  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the StockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON)
    THEN ${catalog_name}.curated_inventory.get_sum(MSKA.KALAB, MSKA.KAINS, MSKA.KAEIN, MSKA.KASPE, 0)
    ELSE ${catalog_name}.curated_inventory.get_sum(MSKAH.KALAB, MSKAH.KAINS, MSKAH.KAEIN, MSKAH.KASPE, 0)
    END AS StockQuantity,

  --Unit Price Of a Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the Unit Price Of a Stock calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END AS UnitPrice,
    -- TOTAL STOCK
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_sum(MSKA.KALAB, MSKA.KAINS, MSKA.KAEIN, MSKA.KASPE, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_sum(MSKAH.KALAB, MSKAH.KAINS, MSKAH.KAEIN, MSKAH.KASPE, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS StockValue,


  --Unrestricted Stock 
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */ 
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KALAB)
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KALAB)
    END AS UnrestrictedStockQuantity,

  /* The valuation of UnrestrictedStock involves multiplying the UnrestrictedStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KALAB) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KALAB) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS UnrestrictedStockValue,

    -- stock in Transit
    0 StockInTransitQuantity,
    0 StockInTransitvalue,

  --quality Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the QualityInspectionQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KAINS)
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KAINS)
    END AS QualityInspectionQuantity,

    /* The valuation of QualityInspection stock involves multiplying the QualityInspectionQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KAINS) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KAINS) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS QualityInspectionValue,

    --Blocked Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BlockedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KASPE)
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KASPE)
    END AS BlockedStockQuantity,
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KASPE) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KASPE) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BlockedStockValue,

    --restricted Stock
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KAEIN)
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KAEIN)
    END AS BatchRestrictedStockQuantity,
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSKA.LFGJA, MSKA.LFMON) 
    THEN ${catalog_name}.curated_inventory.get_num(MSKA.KAEIN) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSKAH.KAEIN) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BatchRestrictedStockValue,

    --return Stock

  0 ReturnsStockQuantity,
  0 ReturnsStockValue,
    MARA.summary_indicator as MARA_ABC_Analysis,
    MARC.abc_indicator as MARC_ABC_Analysis
  FROM ${catalog_name}.curated_sap.tbl_MSKA_curated_scd1 AS MSKA
  left join MBEW
  on MSKA.material_num = MBEW.material_number and MSKA.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON MSKA.material_num = MARA.material_number
  left join MARC
  on MSKA.material_num = MARC.material_number and MSKA.plant = MARC.plant
  /* A cross join is performed on the time dimension, focusing on dates from past 3 years up  to the present. 
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month. 
    The last calendar date is selected for each fiscal month. */
  CROSS JOIN (select to_date(max(`day`)) as Calander_Date, ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from ${catalog_name}.consumption_masterdata.vw_dp_md_time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<getdate() group by ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date

  LEFT JOIN MSKAH
  ON MSKA.material_num = MSKAH.material_num AND MSKA.plant = MSKAH.plant AND MSKA.storage_location = MSKAH.storage_location AND MSKA.batch_num = MSKAH.batch_num AND MSKA.special_stock_ind = MSKAH.special_stock_ind AND MSKA.sales_distribution_document_num = MSKAH.sales_distribution_document_num AND MSKA.sd_document_item_num = MSKAH.sd_document_item_num AND (MSKAH.date = Date.Date OR (Date.Date > MSKAH.Date AND Date.DATE < MSKAH.NextDate))

  LEFT JOIN MBEWH
  ON MBEWH.material_number = MSKA.material_num AND MSKA.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate))


-- COMMAND ----------

------------------------------------------------LOGIC FOR MSLB AND MSLBH-----------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW special_stocks_with_vendor AS 
  SELECT
  YEAR(Date.Date) AS Year, --fiscal year
  Month(Date.Date) AS Month, --fiscal year
  Date.Date AS Date, --Date created by fiscal year and fiscal month
  date.Calander_Date, --last calander date for the fiscal month
  MSLB.material_number AS MaterialId, -- Material ID
  MSLB.plant AS PlantSLocID, -- Concatenating Plant and Storage Location IDs
  --replace(ltrim(replace(MSLB.issue_loc_for_prod_order,'0',' ')),' ','0')) AS PlantSLocID,
  MSLB.plant AS PlantId, -- Plant ID
  --replace(ltrim(replace(MSLB.issue_loc_for_prod_order,'0',' ')),' ','0') 
  '' AS StorageLocationId, -- Storage Location ID
  MSLB.batch_number AS BatchId, -- Batch ID
  MARA.base_uom AS BaseUnit, -- Base Unit of Measurement

  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the StockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period)
    THEN ${catalog_name}.curated_inventory.get_sum(MSLB.valuated_unrestricted_use_stock, MSLB.stock_in_qual_inspection,  MSLB.restricted_batches_total_stock, 0, 0)
    ELSE ${catalog_name}.curated_inventory.get_sum(MSLBH.LBLAB, MSLBH.LBINS, MSLBH.LBEIN, 0, 0)
    END AS StockQuantity,

  --Unit Price Of a Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the Unit Price Of a Stock calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END AS UnitPrice,
    -- TOTAL STOCK
  CASE WHEN Date.DATE >= last_day(CONCAT(MSLB.current_period_fiscal_year,'-',MSLB.current_period, '-','1')) 
    THEN ${catalog_name}.curated_inventory.get_sum(MSLB.valuated_unrestricted_use_stock, MSLB.stock_in_qual_inspection,  MSLB.restricted_batches_total_stock, 0, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_sum(MSLBH.LBLAB, MSLBH.LBINS, MSLBH.LBEIN, 0, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS StockValue,

  --Unrestricted Stock 
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MSLB.valuated_unrestricted_use_stock)
    ELSE ${catalog_name}.curated_inventory.get_num(MSLBH.LBLAB)
    END AS UnrestrictedStockQuantity,

  /* The valuation of UnrestrictedStock involves multiplying the UnrestrictedStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period)
    THEN ${catalog_name}.curated_inventory.get_num(MSLB.valuated_unrestricted_use_stock) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSLBH.LBLAB) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS UnrestrictedStockValue,

    -- stock in Transit
    0 StockInTransitQuantity,
    0 StockInTransitvalue,

  --quality Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the QualityInspectionQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MSLB.stock_in_qual_inspection)
    ELSE ${catalog_name}.curated_inventory.get_num(MSLBH.LBINS)
    END AS QualityInspectionQuantity,

    /* The valuation of QualityInspection stock involves multiplying the QualityInspectionQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MSLB.stock_in_qual_inspection) * CASE WHEN Date.DATE >= 
    ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period) 
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSLBH.LBINS) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS QualityInspectionValue,

    --Blocked Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BlockedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  0 as BlockedStockQuantity,
  0 as BlockedStockValue,

    --Restricted Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the BatchRestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MSLB.restricted_batches_total_stock)
    ELSE ${catalog_name}.curated_inventory.get_num(MSLBH.LBEIN)
    END AS BatchRestrictedStockQuantity,
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MSLB.current_period_fiscal_year, MSLB.current_period)
    THEN ${catalog_name}.curated_inventory.get_num(MSLB.restricted_batches_total_stock) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MSLBH.LBEIN) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS BatchRestrictedStockValue,

    --return Stock
  0 as ReturnsStockQuantity,
  0 as ReturnsStockValue,
    MARA.summary_indicator as MARA_ABC_Analysis,
    MARC.abc_indicator as MARC_ABC_Analysis
  FROM ${catalog_name}.curated_sap.tbl_MSLB_curated_scd1 AS MSLB
  left join MBEW
  on MSLB.material_number = MBEW.material_number and MSLB.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON MSLB.material_number = MARA.material_number
  left join MARC
  on MSLB.material_number = MARC.material_number and MSLB.plant = MARC.plant
  /* A cross join is performed on the time dimension, focusing on dates from past 3 years up  to the present. 
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month. 
    The last calendar date is selected for each fiscal month. */
  CROSS JOIN (select to_date(max(`day`)) as Calander_Date, ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from ${catalog_name}.consumption_masterdata.vw_dp_md_time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<getdate()  group by ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date

  LEFT JOIN MSLBH
  ON MSLB.material_number = MSLBH.material_num AND MSLB.plant = MSLBH.plant AND MSLB.batch_number = MSLBH.batch_num AND MSLB.special_stock_indicator = MSLBH.special_stock_ind AND MSLB.vendor_account_number = MSLBH.vendor_account_num AND (MSLBH.date = Date.Date OR (Date.Date > MSLBH.Date AND Date.DATE < MSLBH.NextDate))

  LEFT JOIN (select *, ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period) AS Date, 
  COALESCE(LEAD(${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)) OVER(PARTITION BY valuation_area,material_number ORDER BY ${catalog_name}.curated_inventory.get_last_day(current_period_fiscal_year, current_period)),GETDATE()) AS NextDate from ${catalog_name}.curated_sap.tbl_mbewh_curated_scd1 WHERE valuation_type = " ") MBEWH
  ON MBEWH.material_number = MSLB.material_number AND MSLB.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate))

-- COMMAND ----------

------------------------------------------------LOGIC FOR MARC AND MARCH--------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW material_master_plant AS 
  SELECT
  YEAR(Date.Date) AS Year, --fiscal year
  Month(Date.Date) AS Month, --fiscal year
  Date.Date AS Date, --Date created by fiscal year and fiscal month
  date.Calander_Date, --last calander date for the fiscal month
  MARC.material_number AS MaterialId, -- Material ID
  MARC.plant AS PlantSLocID, -- Concatenating Plant and Storage Location IDs
  --replace(ltrim(replace(MARC.issue_loc_for_prod_order,'0',' ')),' ','0')) AS PlantSLocID,
  MARC.plant AS PlantId, -- Plant ID
  --replace(ltrim(replace(MARC.issue_loc_for_prod_order,'0',' ')),' ','0') 
  '' AS StorageLocationId, -- Storage Location ID
  '' AS BatchId, -- Batch ID
  MARA.base_uom AS BaseUnit, -- Base Unit of Measurement

  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the StockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MARC.current_period_fiscal_year, MARC.current_period) 
    THEN ${catalog_name}.curated_inventory.get_sum(MARC.stock_in_transit, 0, 0, 0, 0)
    ELSE ${catalog_name}.curated_inventory.get_sum(MARCH.stock_in_transit, 0, 0, 0, 0)
    END AS StockQuantity,
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END AS UnitPrice,
    ---------TOTAL STOCK----------
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MARC.current_period_fiscal_year, MARC.current_period) 
    THEN ${catalog_name}.curated_inventory.get_sum(MARC.stock_in_transit, 0, 0, 0, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_sum(MARCH.stock_in_transit, 0, 0, 0, 0) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS StockValue,

  --Unrestricted Stock 
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */ 

  0 AS UnrestrictedStockQuantity,
  0 AS UnrestrictedStockValue,

  ----STOCK IN TRANSIT------------
    CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MARC.current_period_fiscal_year, MARC.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MARC.stock_in_transit)
    ELSE ${catalog_name}.curated_inventory.get_num(MARCH.stock_in_transit)
    END AS StockInTransitQuantity,

  /* The valuation of UnrestrictedStock involves multiplying the UnrestrictedStockQuantity by the unit price. */
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MARC.current_period_fiscal_year, MARC.current_period) 
    THEN ${catalog_name}.curated_inventory.get_num(MARC.stock_in_transit) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    ELSE ${catalog_name}.curated_inventory.get_num(MARCH.stock_in_transit) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
    THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
    ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
    END
    END AS StockInTransitvalue,
    --0 StockInTransitQuantity,
    --0 StockInTransitvalue,

  --quality Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the QualityInspectionQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  0 AS QualityInspectionQuantity,
  0 AS QualityInspectionValue,

  ----BLOCKED STOCK---------

  0 as BlockedStockQuantity,
  0 as BlockedStockValue,

  ---------RESTRICTED STOCK---------
  0 AS BatchRestrictedStockQuantity,
  0 AS BatchRestrictedStockValue,

  -------RETURN STOCK---------------
  0 as ReturnsStockQuantity,
  0 as ReturnsStockValue,
  MARA.summary_indicator as MARA_ABC_Analysis,
  MARC.abc_indicator as MARC_ABC_Analysis
  FROM MARC
  left join MBEW
  on MARC.material_number = MBEW.material_number and MARC.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON MARC.material_number = MARA.material_number
  --left join (select * from ${catalog_name}.curated_sap.tbl_marc_curated_scd2 where `__END_AT` is null) MARC
  --on MARC.material_number = MARC.material_number and MARC.plant = MARC.plant

  /* A cross join is performed on the time dimension, focusing on dates from past 3 years up  to the present. 
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month. 
    The last calendar date is selected for each fiscal month. */
  CROSS JOIN (select to_date(max(`day`)) as Calander_Date, ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from ${catalog_name}.consumption_masterdata.vw_dp_md_time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<getdate()  group by ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date

  LEFT JOIN MARCH
  ON MARC.material_number = MARCH.material_number AND MARC.plant = MARCH.plant AND (MARCH.date = Date.Date OR (Date.Date > MARCH.Date AND Date.DATE < MARCH.NextDate))

  LEFT JOIN MBEWH
  ON MBEWH.material_number = MARC.material_number AND MARC.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate)) 


-- COMMAND ----------

----------------------------------------------------LOGIC FOR LIPS------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW delivery_stocks AS 
  SELECT
  YEAR(Date.Date) AS Year, --fiscal year
  Month(Date.Date) AS Month, --fiscal year
  Date.Date AS Date, --Date created by fiscal year and fiscal month
  date.Calander_Date, --last calander date for the fiscal month
  lips.matr_num AS MaterialId, -- Material ID
  concat(lips.plant,lips.issue_loc_for_prod_order) AS PlantSLocID, -- Concatenating Plant and Storage Location IDs
  lips.plant AS PlantId, -- Plant ID
  lips.issue_loc_for_prod_order AS StorageLocationId, -- Storage Location ID
  lips.batch_num AS BatchId, -- Batch ID
  MARA.base_uom AS BaseUnit, -- Base Unit of Measurement
  --On Hand stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the StockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */

  0 as  StockQuantity,

  --Unit Price Of a Stock
  CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
  THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
  ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
  END AS UnitPrice,

  -- ON hand stock Value
  0 AS StockValue,

  --Unrestricted Stock 
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period, 
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done 
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  0 AS UnrestrictedStockQuantity,
  0 AS UnrestrictedStockValue,

  -- stock in Transit
  0 StockInTransitQuantity,
  0 StockInTransitvalue,

  --quality Stock
  0 AS QualityInspectionQuantity,
  0 AS QualityInspectionValue,

  --Blocked Stock
  0 AS BlockedStockQuantity,
  0 AS BlockedStockValue,

  --Restricted Stock
  0 AS BatchRestrictedStockQuantity,
  0 AS BatchRestrictedStockValue,

  --Return Stock
  CASE WHEN date.Calander_Date >= lips.record_created_on
  THEN ${catalog_name}.curated_inventory.get_num(lips.actual_quan_delivered_in_sales_units)
  ELSE 0
  END AS ReturnsStockQuantity,
  CASE WHEN date.Calander_Date >= lips.record_created_on 
  THEN ${catalog_name}.curated_inventory.get_num(lips.actual_quan_delivered_in_sales_units) * CASE WHEN Date.DATE >= ${catalog_name}.curated_inventory.get_last_day(MBEW.current_period_fiscal_year, MBEW.current_period)
  THEN ${catalog_name}.curated_inventory.get_division(MBEW.value_of_total_valuated_stock, MBEW.total_valued_stock)
  ELSE ${catalog_name}.curated_inventory.get_division(MBEWH.value_of_total_valuated_stock, MBEWH.total_valued_stock)
  END
  ELSE 0
  END AS ReturnsStockValue,
  MARA.summary_indicator as MARA_ABC_Analysis,
  MARC.abc_indicator as MARC_ABC_Analysis
  FROM LIPS
  left join MBEW
  on lips.matr_num = MBEW.material_number and lips.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON lips.matr_num = MARA.material_number
  left join MARC
  on lips.matr_num = MARC.material_number and lips.plant = MARC.plant

  CROSS JOIN (select to_date(max(`day`)) as Calander_Date, ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from ${catalog_name}.consumption_masterdata.vw_dp_md_time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<=getdate()  group by ${catalog_name}.curated_inventory.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date

  LEFT JOIN MBEWH
  ON lips.matr_num=MBEWH.material_number AND lips.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate))

-- COMMAND ----------

---------------------------------------------------Union of all views------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW vw_combined AS 
  SELECT * from batch_master
  UNION
  SELECT * from special_stocks_from_vendor
  UNION 
  SELECT * from sales_order_stock
  UNION 
  SELECT * from special_stocks_with_vendor
  UNION
  SELECT * from material_master_plant
  UNION
  SELECT * from delivery_stocks

-- COMMAND ----------

INSERT OVERWRITE ${catalog_name}.curated_inventory.tbl_inventory_fact
  SELECT
    Year as year,
    Month as month,
    Date as date,
    Calander_Date as calender_date,
    MaterialId as material_id,
    PlantSLocID as plant_storage_loc_id,
    PlantId as plant_id,
    StorageLocationId as storage_loc_id,
    BatchId as batch_id,
    BaseUnit as base_unit,
    sum(StockQuantity) as stock_qty,
    UnitPrice as unit_price,
    sum(StockValue) as stock_value,
    sum(UnrestrictedStockQuantity) as unrestricted_stock_qty,
    sum(UnrestrictedStockValue) as unrestricted_stock_value,
    sum(StockInTransitQuantity) as stock_transit_qty,
    sum(StockInTransitvalue) as stock_transit_value,
    sum(QualityInspectionQuantity) as quality_inspection_qty,
    sum(QualityInspectionValue) as quality_inspection_value,
    sum(BlockedStockQuantity) as blocked_stock_qty,
    sum(BlockedStockValue) as blocked_stock_value,
    sum(BatchRestrictedStockQuantity) as batch_restricted_stock_qty,
    sum(BatchRestrictedStockValue) as batch_restricted_stock_value,
    sum(ReturnsStockQuantity) as returns_stock_qty,
    sum(ReturnsStockValue) as returns_stock_value,
    MARA_ABC_Analysis as mara_abc_analysis,
    MARC_ABC_Analysis as marc_abc_analysis
  from vw_combined group by year, month, date, calender_date, material_id, plant_storage_loc_id, plant_id,storage_loc_id, batch_id, base_unit, unit_price, mara_abc_analysis, marc_abc_analysis

-- COMMAND ----------

-- Create view
CREATE OR REPLACE VIEW ${catalog_name}.curated_inventory.vw_inventory_fact  AS
SELECT * FROM ${catalog_name}.curated_inventory.tbl_inventory_fact ;
