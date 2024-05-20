{{ config(materialized='incremental') }}
with kna1 as
(
    select client, customer_number
    from {{ref('KNA1')}}
)

select 
kna1.customer_number as cust_number
-- knvv.KDGRP as cust_segment

from 
kna1
-- join knvv on kna1.customer_number = knvv.KUNNR



-- Field names	source tables
-- Cust number	KNA1
-- Cust segment	KNVV