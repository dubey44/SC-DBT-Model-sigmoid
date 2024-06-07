-- from ${snowflake_catalog_name}.raw_otm.TBL_OTM_SHIPMENT shipment
-- left join OTM_SHIPMENT_STOP_TYPE_P
-- on shipment_stop_SHIPMENT_GID_P = shipment.SHIPMENT_GID
-- left join OTM_SHIPMENT_STOP_TYPE_D
-- on shipment_stop_SHIPMENT_GID_D = shipment.SHIPMENT_GID
-- left join ${snowflake_catalog_name}.raw_otm.TBL_OTM_LOCATION otm_location_source
-- on shipment.SOURCE_LOCATION_GID = otm_location_source.LOCATION_GID
-- left join ${snowflake_catalog_name}.raw_otm.TBL_OTM_LOCATION otm_location_dest
-- on shipment.DEST_LOCATION_GID = otm_location_dest.LOCATION_GID
-- left join ${snowflake_catalog_name}.raw_otm.TBL_OTM_SHIPMENT_REFNUM shipment_refnum
-- on shipment_refnum.SHIPMENT_GID = shipment.SHIPMENT_GID
{{ config(materialized='incremental') }}
with TBL_OTM_SHIPMENT as(
  select * from {{ref('OTM_SHIPMENT')}}
),
OTM_SHIPMENT_STOP_TYPE_P as(
  select * from {{ref('otm_shipment_stop_type_p')}}
),
OTM_SHIPMENT_STOP_TYPE_D as(
  select * from {{ref('otm_shipment_stop_type_d')}}
),
TBL_OTM_LOCATION as(
  select * from {{ref('TBL_OTM_LOCATION')}}
),
TBL_OTM_SHIPMENT_REFNUM as(
  select * from {{ref('TBL_OTM_SHIPMENT_REFNUM')}}
)



select 
shipment.SERVPROV_GID as servprov_gid,
shipment.ATTRIBUTE3 as attributes,
shipment.SHIPMENT_TYPE_GID as shipment_type_gid,
shipment.TRANSPORT_MODE_GID as transport_mode_gid,
shipment.RATE_GEO_GID as rate_geo_gid,
shipment.TOTAL_ITEM_PACKAGE_COUNT as total_item_package_count,
shipment.RATE_OFFERING_GID as rate_offering_gid,
shipment.SHIPMENT_RELEASED as shipment_released,
shipment.NUM_ORDER_RELEASES as num_order_releases,
shipment.LAST_EVENT_GROUP_GID as last_event_group_gid,
shipment.DOMAIN_NAME as domain_name,
shipment.WEIGHT_UTILIZATION as weight_utilization,
shipment.VOLUME_UTILIZATION as volume_utilization,
shipment.INSERT_DATE as insert_date,
shipment.SHIPMENT_XID as shipment_id,
otm_location_source.TIME_ZONE_GID as origin_timezone_gid,
otm_location_dest.TIME_ZONE_GID as dest_timezone_gid,

CASE 
WHEN otm_location_source.TIME_ZONE_GID = 'AST'
  THEN 'Etc/GMT+4'
WHEN otm_location_source.TIME_ZONE_GID = 'Japan'
  THEN 'Japan'
ELSE otm_location_source.TIME_ZONE_GID
END AS origin_timezone,

CASE 
WHEN otm_location_dest.TIME_ZONE_GID = 'AST'
  THEN 'Etc/GMT+4'
WHEN otm_location_dest.TIME_ZONE_GID = 'Japan'
  THEN 'Japan'
WHEN otm_location_dest.TIME_ZONE_GID = 'UTC-5'
  THEN 'Etc/GMT+5'
ELSE otm_location_dest.TIME_ZONE_GID
END AS dest_timezone,

OTM_SHIPMENT_STOP_TYPE_P.pickup_eta,
OTM_SHIPMENT_STOP_TYPE_P.pickup_pta,
OTM_SHIPMENT_STOP_TYPE_P.site_pickup_appt,
OTM_SHIPMENT_STOP_TYPE_P.actual_arrival_at_shipper,
OTM_SHIPMENT_STOP_TYPE_P.actual_departure_at_shipper,
OTM_SHIPMENT_STOP_TYPE_D.delivery_eta,
OTM_SHIPMENT_STOP_TYPE_D.delivery_pta,
OTM_SHIPMENT_STOP_TYPE_D.site_delivery_appt,
OTM_SHIPMENT_STOP_TYPE_D.actual_arrival_at_consignee,
OTM_SHIPMENT_STOP_TYPE_D.actual_departure_at_consignee,
shipment_refnum.SHIPMENT_REFNUM_VALUE as shipment_refnum_value
from TBL_OTM_SHIPMENT shipment
left join OTM_SHIPMENT_STOP_TYPE_P
on shipment_stop_SHIPMENT_GID_P = shipment.SHIPMENT_GID
left join OTM_SHIPMENT_STOP_TYPE_D
on shipment_stop_SHIPMENT_GID_D = shipment.SHIPMENT_GID
left join TBL_OTM_LOCATION otm_location_source
on shipment.SOURCE_LOCATION_GID = otm_location_source.LOCATION_GID
left join TBL_OTM_LOCATION otm_location_dest
on shipment.DEST_LOCATION_GID = otm_location_dest.LOCATION_GID
left join TBL_OTM_SHIPMENT_REFNUM shipment_refnum
on shipment_refnum.SHIPMENT_GID = shipment.SHIPMENT_GID
where shipment_refnum.SHIPMENT_REFNUM_QUAL_GID = 'BM'