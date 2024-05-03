-- {{ config(materialized='table') }}
-- with kna1 as
-- (
--     select KUNNR, BBBNR, BBBNR, BRSCH 
--     from {{ref('LFA1')}}
-- )



-- MANDT

-- Field names	source tables
-- Cust number	KNA1
-- Cust segment	KNVV