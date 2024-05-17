{{ config(materialized='incremental') }}


select *,raw.get_last_day(CAST(current_period_fiscal_year as INT), CAST(current_period as INT)) AS Date, 
  coalesce(LEAD(raw.get_last_day(CAST(current_period_fiscal_year as INT), CAST(current_period as INT)))
  OVER(PARTITION BY valuation_area,material_number
  ORDER BY raw.get_last_day(CAST(current_period_fiscal_year as INT), CAST(current_period as INT))),NOW()) AS NextDate 
  FROM {{ source('raw_source', 'mbewh') }} WHERE valuation_type is NULL