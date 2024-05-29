{{ config(materialized='incremental') }}

with LIKP as
(
    select *
    from {{ref('LIKP')}}
),
LIPS as
(
    select *
    from {{ref('LIPS')}}
)
  select 
    LIPS.document_num_of_ref_document as order_number,
    'R3' as source,
    LIKP.delivery_document as delivery_document,
    max(LIKP.planned_goods_movement_date) as planned_goods_movement_date,
    max(LIKP.actual_goods_movement_date) as actual_goods_movement_date
    from LIKP LEFT JOIN LIPS on LIKP.delivery_document = LIPS.delivery_document and LIKP.client = LIPS.client where LIPS.document_num_of_ref_document <> '0' 
    group by LIPS.document_num_of_ref_document,LIKP.delivery_document

    union all
    
  select 
    LIPS.document_num_of_ref_document as order_number,
    'S4' as source,
    LIKP.delivery_document as delivery_document,
    max(LIKP.planned_goods_movement_date) as planned_goods_movement_date,
    max(LIKP.actual_goods_movement_date) as actual_goods_movement_date
    from LIKP LEFT JOIN LIPS on LIKP.delivery_document = LIPS.delivery_document and LIKP.client = LIPS.client where LIPS.document_num_of_ref_document <> '0' 
   group by LIPS.document_num_of_ref_document,LIKP.delivery_document