{{ config(materialized='incremental') }}

with tbl_otm_ss_status_history as(
    select *
    from {{ref('OTM_SS_STATUS_HISTORY')}}
),

tbl_otm_ie_shipmentstatus as(
    select * from {{ref('OTM_IE_SHIPMENTSTATUS')}}
)

select 
  shipment_gid,
  insert_date,
  status_code_gid,
  eventdate,
  i_transaction_no,
  shipment_status_type
from
    (select *
    from
    (   select hist.shipment_gid,
                hist.insert_date,
                status.status_code_gid,
                status.eventdate,
                hist.i_transaction_no,
                status.shipment_status_type,
                row_number() over(partition by hist.shipment_gid,status.status_code_gid order by hist.insert_date desc) as rn
        from tbl_otm_ss_status_history as hist
        left join tbl_otm_ie_shipmentstatus as status
        on hist.i_transaction_no = status.i_transaction_no
    )
    where rn = 1)
