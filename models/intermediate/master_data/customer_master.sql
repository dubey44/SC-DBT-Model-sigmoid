{{ config(materialized='table') }}
with kna1 as
(
    select MANDT, KUNNR
    from {{ref('KNA1')}}
),
knvv as(
    select MANDT, KUNNR,KDGRP
    from {{ref('KNVV')}}
)
select 
kna1.KUNNR as cust_number,
knvv.KDGRP as cust_segment

from 
kna1
join knvv on kna1.KUNNR = knvv.KUNNR



-- Field names	source tables
-- Cust number	KNA1
-- Cust segment	KNVV