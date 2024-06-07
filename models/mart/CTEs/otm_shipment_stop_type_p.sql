
with  TBL_OTM_SHIPMENT_STOP as(
    select * from test.raw.otm_shipment_stop
)
select 
shipment_stop.SHIPMENT_GID as shipment_stop_shipment_gid_p,
shipment_stop.ESTIMATED_ARRIVAL as pickup_eta,
shipment_stop.PLANNED_ARRIVAL as pickup_pta,
shipment_stop.APPOINTMENT_PICKUP as site_pickup_appt,
shipment_stop.ACTUAL_ARRIVAL as actual_arrival_at_shipper,
shipment_stop.ACTUAL_DEPARTURE as actual_departure_at_shipper
from TBL_OTM_SHIPMENT_STOP shipment_stop
where shipment_stop.STOP_TYPE ='P' and STOP_NUM = 1