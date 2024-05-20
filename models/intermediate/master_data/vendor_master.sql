{{ config(materialized='incremental') }}

with lfa1 as
(
    select vendor_account_number, international_location_number_1, industry_key 
    from {{ref('LFA1')}}
)

select 
vendor_account_number as vendor_account_no,
international_location_number_1 as geo_location,
industry_key as industry
from lfa1