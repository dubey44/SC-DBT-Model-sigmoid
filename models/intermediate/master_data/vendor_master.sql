{{ config(materialized='incremental') }}

with lfa1 as
(
    select LIFNR, BBBNR, BRSCH 
    from {{ref('LFA1')}}
)

select 
LIFNR as vendor_account_no,
BBBNR as geo_location,
BRSCH as industry
from lfa1