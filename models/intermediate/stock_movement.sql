{{ config(materialized='incremental') }}

with tbl_mseg_curated_scd1 as
(
    select * 
    from {{ref('MSEG')}}
),
tbl_mkpf_curated_scd1 as
(
    select * 
    from {{ref('MKPF')}}
)

SELECT
MSEG.material_document_num_mblnr as material_document_num,
MSEG.material_document_item_zeile as material_document_item,
MSEG.material_document_year_mjahr as material_document_year,
MSEG.material_num AS MaterialID
,MSEG.plant AS PlantID
,MSEG.issue_loc_for_prod_order AS StorageLocationID
,Mseg.batch_num AS BatchId
,MKPF.posting_date_in_doc AS PostingDate
,MSEG.movement_type_inventory_mgmt AS MovementType
,MSEG.competitor AS CustomerID
,MSEG.company_code AS CompanyCode
,CASE WHEN MSEG.returns_item = 'H'
     THEN -(MSEG.component_qty)
     ELSE MSEG.component_qty
END        AS Quantity
,MSEG.base_uom AS BaseUnit
,MSEG.returns_item AS RecieptOrIssue
,MSEG.amount_local_currency AS Value
--Considering the movement type 101 from MSEG table 
FROM (select * from tbl_mseg_curated_scd1 where movement_type_inventory_mgmt='101') AS MSEG
LEFT JOIN tbl_mkpf_curated_scd1 as MKPF
on MSEG.material_document_num_mblnr = MKPF.matr_document_num_mblnr and
  MSEG.material_document_year_mjahr = MKPF.matr_document_year_mjahr
