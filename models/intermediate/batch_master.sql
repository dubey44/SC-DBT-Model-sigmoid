{{ config(materialized='incremental') }}

with MCHB as
(
    select * 
    from {{ref('MCHB')}}
),
MCHBH as(
    select *
    from {{ref('MCHBH')}}
),
MBEW as(
    select *
    from {{ref('MBEW')}}
),
MBEWH as(
    select *
    from {{ref('MBEWH')}}
),
MARA as(
    select *
    from {{ref('MARA')}}
),
MARC as(
    select * 
    from {{ref('MARC')}}
)

  SELECT
  -- YEAR(Date.Date) AS Year,--fiscal year
  EXTRACT(YEAR FROM Date.Date) as Year,
  -- Month(Date.Date) AS Month,--fiscal month
  EXTRACT(MONTH FROM Date.Date) as Month,
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
--------------------------------------------------
  -- CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period as INT))
  --   THEN raw.get_sum(CAST(MCHB.valuated_unrestricted_use_stock as INT),  CAST(MCHB.stock_in_qual_inspection AS INT), CAST(MCHB.restricted_batches_total_stock AS INT), CAST(MCHB.blocked_stock AS INT), CAST(MCHB.blocked_stock_return AS INT))
  --   ELSE raw.get_sum(CAST(MCHBH.CLABS AS INT), CAST(MCHBH.CINSM AS INT), CAST(MCHBH.CEINM AS INT), CAST(MCHBH.CSPEM AS INT), CAST(MCHBH.CRETM AS INT))
  --   END AS StockQuantity,
  raw.get_num(CAST(MCHB.valuated_unrestricted_use_stock as numeric)) AS StockQuantity,
---------------------------------------------------

  --Unit Price Of a Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period,
  the Unit Price Of a Stock calculation is done from the current table. Otherwise, the calculation is done
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year as INT), CAST(MBEW.current_period as INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT),CAST( MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST (MBEWH.total_valued_stock AS INT))
        -- ELSE raw.get_division(0, 1)
    END AS UnitPrice,

    --Stock On Hand Value
    /* The valuation of stock on hand involves multiplying the stock quantity by the unit price. */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year AS INT), CAST(MCHB.current_period AS INT))
    THEN raw.get_sum(CAST(MCHB.valuated_unrestricted_use_stock AS INT),CAST( MCHB.stock_in_qual_inspection AS INT), CAST(MCHB.restricted_batches_total_stock AS INT),CAST( MCHB.blocked_stock AS INT),CAST( MCHB.blocked_stock_return AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST( MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    ELSE raw.get_sum(CAST(MCHBH.CLABS AS INT), CAST(MCHBH.CINSM AS INT), CAST(MCHBH.CEINM AS INT), CAST(MCHBH.CSPEM AS INT), CAST(MCHBH.CRETM AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    END AS StockValue,

  --Unrestricted Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period,
  the UnrestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year AS INT),CAST( MCHB.current_period AS INT))
    THEN raw.get_num(CAST(MCHB.valuated_unrestricted_use_stock AS INT))
    ELSE raw.get_num(CAST(MCHBH.CLABS AS INT))
    END AS UnrestrictedStockQuantity,

    /* The valuation of UnrestrictedStock involves multiplying the UnrestrictedStockQuantity by the unit price. */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year AS INT), CAST(MCHB.current_period AS INT))
    THEN raw.get_num(CAST(MCHB.valuated_unrestricted_use_stock AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST( MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    ELSE raw.get_num(CAST(MCHBH.CLABS AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    END AS UnrestrictedStockValue,

    -- stock in Transit
    0 StockInTransitQuantity,
    0 StockInTransitvalue,

  --quality Stock
  /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period,
  the QualityInspectionQuantity calculation is done from the current table. Otherwise, the calculation is done
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year AS INT), CAST(MCHB.current_period AS INT))
    THEN raw.get_num(CAST(MCHB.stock_in_qual_inspection AS INT))
    ELSE raw.get_num(CAST(MCHBH.CINSM AS INT))
    END AS QualityInspectionQuantity,
   
    /* The valuation of QualityInspection stock involves multiplying the QualityInspectionQuantity by the unit price. */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year AS INT), CAST(MCHB.current_period AS INT)) 
    THEN raw.get_num(CAST(MCHB.stock_in_qual_inspection AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT),CAST( MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    ELSE raw.get_num(CAST(MCHBH.CINSM AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    END AS QualityInspectionValue,

    --Blocked Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period,
  the BlockedStockQuantity calculation is done from the current table. Otherwise, the calculation is done
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period  AS INT))  
    THEN raw.get_num(CAST(MCHB.blocked_stock as INT))
    ELSE raw.get_num(CAST(MCHBH.CSPEM AS INT))
    END AS BlockedStockQuantity,

    /* The valuation of BlockedStock involves multiplying the BlockedStockQuantity by the unit price. */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period AS INT))
    THEN raw.get_num(CAST(MCHB.blocked_stock as INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year as INT), CAST(MBEW.current_period as INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    ELSE raw.get_num(CAST(MCHBH.CSPEM AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    END AS BlockedStockValue,

    --Restricted Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period,
  the BatchRestrictedStockQuantity calculation is done from the current table. Otherwise, the calculation is done
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period AS INT))
    THEN raw.get_num(CAST(MCHB.restricted_batches_total_stock as INT))
    ELSE raw.get_num(CAST(MCHBH.CEINM AS INT))
    END AS BatchRestrictedStockQuantity,

    /* The valuation of BatchRestrictedStock involves multiplying the BatchRestrictedStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period AS INT))  
    THEN raw.get_num(CAST(MCHB.restricted_batches_total_stock as INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year as INT), CAST(MBEW.current_period as INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    ELSE raw.get_num(CAST(MCHBH.CEINM AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    END AS BatchRestrictedStockValue,

    --Return Stock
    /*If the fiscal year and fiscal month of the entry are beyond the current fiscal period,
  the ReturnsStockQuantity calculation is done from the current table. Otherwise, the calculation is done
  from the historical table.Comparision is done by creating a date column using fiscal year and fiscal month */
  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period AS INT))
    THEN raw.get_num(CAST(MCHB.blocked_stock_return as INT))
    ELSE raw.get_num(CAST(MCHBH.CRETM AS INT))
    END AS ReturnsStockQuantity,

    /* The valuation of ReturnStock involves multiplying the ReturnsStockQuantity by the unit price. */

  CASE WHEN Date.DATE >= raw.get_last_day(CAST(MCHB.current_period_fiscal_year as INT), CAST(MCHB.current_period AS INT))  
    THEN raw.get_num(CAST(MCHB.blocked_stock_return as INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    ELSE raw.get_num(CAST(MCHBH.CRETM AS INT)) * CASE WHEN Date.DATE >= raw.get_last_day(CAST(MBEW.current_period_fiscal_year AS INT), CAST(MBEW.current_period AS INT))
    THEN raw.get_division(CAST(MBEW.value_of_total_valuated_stock AS INT), CAST(MBEW.total_valued_stock AS INT))
    ELSE raw.get_division(CAST(MBEWH.value_of_total_valuated_stock AS INT), CAST(MBEWH.total_valued_stock AS INT))
    -- ELSE raw.get_division(0, 1)
    END
    END AS ReturnsStockValue,
    MARA.summary_indicator as MARA_ABC_Analysis,
    MARC.abc_indicator as MARC_ABC_Analysis
  FROM  MCHB
  left join MBEW
  on MCHB.material_number = MBEW.material_number and MCHB.plant = MBEW.valuation_area

  LEFT JOIN MARA
  ON MCHB.material_number = MARA.material_number
  left join MARC
  on MCHB.material_number = MARC.material_number and MCHB.plant = MARC.plant
  /* A cross join is performed on the time dimension, focusing on dates from past 3 years up to the present.
    Each entry represents one fiscal year and month, with a new date column created using the fiscal year and month.
    The last calendar date is selected for each fiscal month. */
--   CROSS JOIN (select to_date(max('day')) as Calander_Date, raw.get_last_day(FISCAL_YEAR, FISCAL_PERIOD) AS Date from test.raw.time_dimension where `day`>=DATEADD(YEAR, -3, GETDATE()) and `day`<getdate() group by raw.get_last_day(FISCAL_YEAR, FISCAL_PERIOD)) Date
CROSS JOIN (
    SELECT 
        MAX("day") AS Calander_Date, 
        test.raw.get_last_day(CAST(fiscal_year AS INT), CAST(fiscal_period AS INT)) AS Date 
    FROM 
        test.raw.time_dimension 
    WHERE 
        "day" >= CURRENT_DATE - INTERVAL '3 year' 
        AND "day" < CURRENT_DATE 
    GROUP BY 
        test.raw.get_last_day(CAST(fiscal_year AS INT), CAST(fiscal_period AS INT))
) AS Date
  /* When sourcing data from the historical table, we select the closest previous entry to the fiscal period. */
  LEFT JOIN  MCHBH
  ON MCHB.material_number = MCHBH.material_num AND MCHB.plant = MCHBH.plant AND MCHB.issue_loc_for_prod_order = MCHBH.storage_location AND MCHB.batch_number = MCHBH.batch_num AND (MCHBH.date = Date.Date OR (Date.Date > MCHBH.Date AND Date.DATE < MCHBH.NextDate))
  /* When sourcing data from the historical table, we select the closest previous entry to the fiscal period. */
  LEFT JOIN  MBEWH
  ON MBEWH.material_number = MCHB.material_number AND MCHB.plant = MBEWH.valuation_area AND (MBEWH.date = Date.Date OR (Date.Date > MBEWH.Date AND Date.DATE < MBEWH.NextDate))
