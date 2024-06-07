
with  TBL_OTM_SHIPMENT_STOP as(
    select * from test.raw.otm_shipment_stop
)

select 
shipment_stop.SHIPMENT_GID as shipment_stop_shipment_gid_d,
shipment_stop.ESTIMATED_ARRIVAL as delivery_eta,
shipment_stop.PLANNED_ARRIVAL as delivery_pta,
shipment_stop.APPOINTMENT_PICKUP as site_delivery_appt,
shipment_stop.ACTUAL_ARRIVAL as actual_arrival_at_consignee,
shipment_stop.ACTUAL_DEPARTURE as actual_departure_at_consignee
from TBL_OTM_SHIPMENT_STOP shipment_stop
where shipment_stop.STOP_TYPE ='D' and STOP_NUM = 2