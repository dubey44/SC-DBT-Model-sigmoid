{{ config(materialized='table') }}

select
    *
from
    {{ source('public_source', 'raw_sap_tbl_ekko_cdf_base') }}