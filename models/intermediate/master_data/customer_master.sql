{{ config(materialized='table') }}
with kna1 as
(
    select MANDT KUNNR
    from {{ref('KNA1')}}
),
knvv as(
    select MANDT, KUNNR
)
select 
kna1.KUNNR as cust_number,
knvv.X as cust_segment

from 
kna1
join knvv on kna1.KUNNR = knvv.KUNNR



-- Field names	source tables
-- Cust number	KNA1
-- Cust segment	KNVV