{{ config(materialized='incremental') }}

SELECT *,raw.get_last_day(CAST(current_period_fiscal_year as INT), CAST(current_period as INT)) AS Date,
      coalesce(LEAD(raw.get_last_day(CAST(current_period_fiscal_year as INT), CAST(current_period as INT)))
            OVER(PARTITION BY plant,storage_location,material_num 
            ORDER BY raw.get_last_day(CAST(current_period_fiscal_year as INT), CAST(current_period as INT))),NOW()) AS NextDate 
            from {{ source('raw_source', 'mchbh') }}
