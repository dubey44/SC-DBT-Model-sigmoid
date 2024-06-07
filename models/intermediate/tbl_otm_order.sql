with tbl_otm_order_release as(
    select * from test.raw.tbl_otm_order_release
),
tbl_otm_order_movement as(
    select * from test.raw.tbl_otm_order_movement
)


select
SPLIT_PART(movement.SHIPMENT_GID,'.',2) AS shipment_no,
release.ORDER_RELEASE_TYPE_GID as order_release_type,
release.DEST_LOCATION_GID as destination_location_id,
release.TOTAL_SHIP_UNIT_COUNT as total_shipment_unit_count,
release.TOTAL_WEIGHT as total_weight,
release.TOTAL_WEIGHT_UOM_CODE as total_weight_uom_code,
release.TOTAL_VOLUME as total_volume,
release.TOTAL_VOLUME_UOM_CODE as total_volume_uom_code,
release.PRIORITY as priority,
release.MODE_PROFILE_GID as mode_profile_gid,
SPLIT_PART(release.ORDER_RELEASE_GID,'.',2) AS order_no
from tbl_otm_order_release release
left join tbl_otm_order_movement movement
on release.ORDER_RELEASE_GID = movement.ORDER_RELEASE_GID