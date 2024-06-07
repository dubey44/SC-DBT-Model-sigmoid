with TBL_OTM_SHIPMENT_STATUS as(
    select * from {{ref('OTM_shipment_status')}}
)

SELECT SHIPMENT_GID,
                status_code_gid,
                SPLIT_PART(STATUS_VALUE_GID,'.',2) AS TRANSPORT_LOAD_STATUS
FROM TBL_OTM_SHIPMENT_STATUS 
WHERE STATUS_TYPE_GID LIKE '%ENROUTE%'

