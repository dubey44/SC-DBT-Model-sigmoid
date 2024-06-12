{{ config(materialized='incremental') }}
-- (
with sales_order_header as
(
    select *
    from {{ref('sales_order_header')}}
),
customer_master as(
    select * from {{ref('customer_master')}}
),
delivery_header as(
    select * from {{ref('delivery_header')}}
),
tbl_otm_order as(
    select * from {{ref('tbl_otm_order')}}
),
tbl_otm_shipment as(
    select * from  {{ref('tbl_otm_shipments')}}
),
otm_shipment_status as(
    select * from {{ref('OTM_shipment_status')}}
),
otm_contact as(
    select * from {{ref('OTM_CONTACT')}}
),
open_order_cte as(
    select * from {{ref('open_order_cte')}}
),
gross_value_cte as(
    select * from {{ref('gross_value_cte')}}
),
csu_cte as(
    select * from {{ref('csu_cte')}}
),
origin_cte as(
    select * from {{ref('origin_cte')}}
)
-- load_status_cte as(
--     select * from {{ref('load_status_cte')}}
-- )
select
-- case when sales_order_header.sales_document IN (SELECT * FROM open_order_cte) then 'OpenOrder'
--             else NULL end as open_order_indicator,
--     case when {{this}}.open_order_indicator is null then 'Closed'
--             when ship_date < CURRENT_DATE then 'A - Past Due'
--             when ship_date = CURRENT_DATE then 'B - Today'
--             when DATEDIFF(TO_DATE(ship_date),CURRENT_DATE) = 1 then 'C +1'
--             when DATEDIFF(TO_DATE(ship_date),CURRENT_DATE) = 2 then 'D +2'
--             when DATEDIFF(TO_DATE(ship_date),CURRENT_DATE) = 3 then 'E +3'
--             else 'F >3' 
--     end as shipdate_designation,

-- case 
--     when on_block = 1 then 'Blocked'
--     when open_order_indicator = 'OpenOrder' then 'OpenOrder'
--     else 'Shipped/Closed' end as status_designation,

sales_order_header.source as source,
sales_order_header.client as client,
sales_order_header.sales_document as order_no,
tbl_otm_order.shipment_no as shipment_no,
split_part(tbl_otm_shipment.servprov_gid,'.',2) as scac,
-- sales_order_header.order_type as order_type,
split_part(tbl_otm_shipment.attributes,'.',2) as otm_order_type,
split_part(tbl_otm_shipment.shipment_type_gid,'.',2) as shipment_type,
tbl_otm_order.order_no as otm_order_release_type_gid,
sales_order_header.shipping_conditions as trans_type,
-- case
--     when order_type in ('KL','ZCON','ZMPL')
--             OR origin_cte.sap_origin_id in ('3926','3883','5568')  -----------------------------------------
--             OR trans_type in ('AF','D') then '3rd Party Managed'
--     when trans_type in ('CE','H','ZC') then 'CPU'
--     when trans_type = 'LT' then 'Delivered_LTL'
--     when trans_type is not null then 'Delivered'
--     else trans_type end AS consolidated_mode,

tbl_otm_shipment.transport_mode_gid as transport_mode_gid,
origin_cte.sap_origin_id as origin_id,
sales_order_header.customer_id as cutomer_id,
customer_master.customer_name as customer_name,
tbl_otm_shipment.shipment_refnum_value as bol,
sales_order_header.net_value_order_item as net_order_value,
gross_value_cte.gross_order_value as gross_order_value,
sales_order_header.total_quantity as order_quantity,
-- csu_cte.quantity_csu as csu_quantity,
tbl_otm_shipment.total_item_package_count as total_item_package_count,
sales_order_header.requested_delivery_date as requested_delivery_date,
-- case  when DATEDIFF(DAY,requested_delivery_date,CURRENT_DATE) > 4 then '>120 Hours Past Due RDD'
case  when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) > 4 then '>120 Hours Past Due RDD'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) > 3 then '96 Hours Past Due RDD'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) > 2 then '72 Hours Past Due RDD'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) > 1 then '48 Hours Past Due RDD'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) > 0 then '24 Hours Past Due RDD'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) <= -6 then 'RDD >120 Hours'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) <= -5 then 'RDD 120 Hours'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) <= -4 then 'RDD 96 Hours'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) <= -3 then 'RDD 72 Hours'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) <= -2 then 'RDD 48 Hours'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) <= -1 then 'RDD 24 Hours'
            when EXTRACT(DAY FROM AGE(CURRENT_DATE, requested_delivery_date)) = 0 then 'Today'
    end as aging_rdd,
sales_order_header.original_rdd as original_requested_delivery_date,
-- delivery_header.planned_goods_movement_date as planned_goods_movement_date,
-- delivery_header.actual_goods_movement_date as actual_goods_movement_date,
sales_order_header.goods_issue_date as planned_goods_issue_date,
-- CONVERT_TIMEZONE(otm_shipment_status.ORIGIN_TIMEZONE,OTM_PGI.EVENTDATE) as otm_planned_goods_issue_date,
l1.EVENTDATE at TIME ZONE ORIGIN_TIMEZONE as otm_planned_goods_issue_date,
-- CONVERT_TIMEZONE(ORIGIN_TIMEZONE,PU.PICKUP_ETA) as pickup_eta,
tbl_otm_shipment.PICKUP_ETA at TIME ZONE ORIGIN_TIMEZONE as pickup_eta,
-- CONVERT_TIMEZONE(ORIGIN_TIMEZONE,PU.PICKUP_PTA) as pickup_pta,
tbl_otm_shipment.PICKUP_PTA at TIME ZONE ORIGIN_TIMEZONE as pickup_pta,
-- CONVERT_TIMEZONE(ORIGIN_TIMEZONE,PU.APPOINTMENT_PICKUP) as site_pickup_appt,
tbl_otm_shipment.site_pickup_appt at TIME ZONE ORIGIN_TIMEZONE as site_pickup_appt,
-- CONVERT_TIMEZONE(ORIGIN_TIMEZONE,shipment_status.EVENTDATE) as carrier_pu_appt
xp.EVENTDATE at TIME ZONE ORIGIN_TIMEZONE as carrier_pu_appt,
-- CONVERT_TIMEZONE(DEST_TIMEZONE,DELIVERY_ETA) as delivery_eta,
tbl_otm_shipment.DELIVERY_ETA at TIME ZONE tbl_otm_shipment.dest_timezone_gid AS delivery_eta,
-- CONVERT_TIMEZONE(DEST_TIMEZONE,DELIVERY_PTA) as delivery_pta,
tbl_otm_shipment.DELIVERY_PTA at TIME ZONE tbl_otm_shipment.dest_timezone_gid as delivery_pta,
-- CONVERT_TIMEZONE(DEST_TIMEZONE,site_delivery_appt) as site_delivery_appt,
tbl_otm_shipment.site_delivery_appt at TIME ZONE tbl_otm_shipment.dest_timezone_gid as site_delivery_appt,
-- CONVERT_TIMEZONE(DEST_TIMEZONE,shipment_status.EVENTDATE) as carrier_del_appt,
xd.EVENTDATE at TIME ZONE DEST_TIMEZONE as carrier_del_appt,
-- CONVERT_TIMEZONE(ORIGIN_TIMEZONE,ACTUAL_ARRIVAL_AT_SHIPPER) as actual_arrival_at_shipper,
tbl_otm_shipment.ACTUAL_ARRIVAL_AT_SHIPPER at TIME ZONE ORIGIN_TIMEZONE as actual_arrival_at_shipper,
-- CONVERT_TIMEZONE(ORIGIN_TIMEZONE,ACTUAL_DEPARTURE_FROM_SHIPPER) as actual_departure_from_shipper
tbl_otm_shipment.actual_departure_at_shipper at TIME ZONE ORIGIN_TIMEZONE as actual_departure_from_shipper,
-- CONVERT_TIMEZONE(DEST_TIMEZONE,ACTUAL_ARRIVAL_AT_CONSIGNEE) as actual_arrival_at_consignee,
tbl_otm_shipment.ACTUAL_ARRIVAL_AT_CONSIGNEE at TIME ZONE DEST_TIMEZONE as actual_arrival_at_consignee,
-- CONVERT_TIMEZONE(DEST_TIMEZONE,ACTUAL_DEPARTURE_FROM_CONSIGNEE) as actual_departure_from_consignee,
tbl_otm_shipment.ACTUAL_DEPARTURE_AT_CONSIGNEE at TIME ZONE DEST_TIMEZONE as actual_departure_from_consignee,
-- CONVERT_TIMEZONE(OTM.DEST_TIMEZONE,shipment_status.EVENTDATE) as carrier_arrived_at_consignee,
x1.EVENTDATE at TIME ZONE DEST_TIMEZONE as carrier_arrived_at_consignee,
tbl_otm_shipment.rate_geo_gid as rate_geo_gid,
tbl_otm_shipment.rate_offering_gid as rate_offering_gid,
-- otm_shipment.shipment_release_status as shipment_release_status,
tbl_otm_shipment.num_order_releases as num_order_releases,
tbl_otm_shipment.last_event_group_gid as last_shipment_status,
d1.status_code_gid as delivered_indicator,
tbl_otm_shipment.domain_name as domain_name,
tbl_otm_order.total_shipment_unit_count as total_shipment_unit_count,
tbl_otm_order.total_weight as total_weight,
tbl_otm_order.total_weight_uom_code as total_weight_uom_code,
tbl_otm_order.total_volume as total_volume,
tbl_otm_order.total_volume_uom_code as total_volume_uom_code,
tbl_otm_shipment.weight_utilization as weight_utilisation,
tbl_otm_shipment.volume_utilization as volume_utilisation,
tbl_otm_order.priority as priority,
tbl_otm_order.mode_profile_gid as mode_profile_gid,
sales_order_header.delivery_block as delivery_block,
sales_order_header.billing_block as billing_block,
case
    when delivery_block <> ' ' OR billing_block <> ' ' then 1
    else 0 end AS on_block,
-- case when sales_order_header.sales_document IN (SELECT * FROM open_order_cte) then 'OpenOrder'
--             else NULL end as open_order_indicator,

-- case when SAP_PGI.EXP_PGI IS NOT NULL then SAP_PGI.EXP_PGI
--             when SAP_PGI.EXP_PGI IS NULL AND SCH_LINE_PGI.PLANNED_GI IS NOT NULL AND SCH_LINE_PGI.PLANNED_GI <> '1900-01-01' then SCH_LINE_PGI.PLANNED_GI
--             when SAP_PGI.EXP_PGI IS NULL AND SCH_LINE_PGI.PLANNED_GI IS NOT NULL AND SCH_LINE_PGI.PLANNED_GI <> '1900-01-01' AND CONSOLIDATED_MODE = 'CPU' then SAP.REQUEST_DELIVERY_DATE
--             else (SAP.REQUEST_DELIVERY_DATE - 3) end as ship_date,
-- case when delivery_header.planned_goods_movement_date is not null then delivery_header.planned_goods_movement_date
--             when delivery_header.planned_goods_movement_date is null and sch_line_pgi.planned_gi is not null and sch_line_pgi.planned_gi <> '1900-01-01' then sch_line_pgi.planned_gi
--             when delivery_header.planned_goods_movement_date is null and sch_line_pgi.planned_gi is not null and sch_line_pgi.planned_gi <> '1900-01-01' and CONSOLIDATED_MODE = 'CPU' then sales_order_header.requested_delivery_date
--             else (sales_order_header.request_delivery_date - 3) end as ship_date,

sales_order_header.record_created_on as order_created_date,
sales_order_header.load_date as load_date

from sales_order_header 
left join customer_master on sales_order_header.customer_id = customer_master.cust_number
left join tbl_otm_order on sales_order_header.customer_purchase_order_number = tbl_otm_order.order_no
left join tbl_otm_shipment on tbl_otm_shipment.shipment_id = tbl_otm_order.shipment_no
left join otm_shipment_status x1 on x1.shipment_gid = tbl_otm_shipment.shipment_id
left join otm_shipment_status d1 on d1.shipment_gid = tbl_otm_shipment.shipment_id
left join otm_shipment_status xp on xp.shipment_gid = tbl_otm_shipment.shipment_id
left join otm_shipment_status xd on xd.shipment_gid = tbl_otm_shipment.shipment_id
left join otm_shipment_status l1 on l1.shipment_gid = tbl_otm_shipment.shipment_id
left join gross_value_cte on gross_value_cte.vbeln = tbl_otm_shipment.shipment_id
left join origin_cte on origin_cte.vbeln = tbl_otm_shipment.shipment_id
--  (shipment_no)
-- join otm_contact on otm_contact

where d1.status_code_gid ='D1'
and x1.status_code_gid ='X1'
and xp.status_code_gid ='XP'
and xd.status_code_gid ='XD'
and l1.status_code_gid ='L1'

-- )  opn
















































