with TBL_OTM_SHIPMENT_STATUS as(
    select * from {{ref('OTM_shipment_status')}}
)

SELECT SHIPMENT_GID,
                status_code_gid,
                SPLIT_PART(shipment_status_type,'.',2) AS TRANSPORT_LOAD_STATUS
FROM TBL_OTM_SHIPMENT_STATUS 
WHERE status_code_gid LIKE '%ENROUTE%'


-- LOAD_STATUS_CTE AS (
-- SELECT SHIPMENT_GID,
--                 STATUS_TYPE_GID,
--                 SPLIT_PART(STATUS_VALUE_GID,'.',2) AS TRANSPORT_LOAD_STATUS
-- FROM snowflake_prd_landzone.RAW_OTM.TBL_OTM_SHIPMENT_STATUS 
-- WHERE STATUS_TYPE_GID LIKE '%ENROUTE%'
-- )