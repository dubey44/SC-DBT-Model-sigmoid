{{ config(materialized='incremental') }}

select
    *
from
    {{ source('raw_source', 'raw_sap_tbl_ekko_cdf_base') }}