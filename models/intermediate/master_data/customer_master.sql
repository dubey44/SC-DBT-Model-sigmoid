{{ config(materialized='table') }}
with lfa1 as
(
    select KNA1, BBBNR, BBBNR, BRSCH 
    from {{ref('LFA1')}}
),


-- Field names	source tables
-- Cust number	KNA1
-- Cust segment	KNVV