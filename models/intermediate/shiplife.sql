{{ config(materialized='incremental') }}

with MCH1 as
(
    select * 
    from {{ref('MCH1')}}
),
MARA as(
    select * 
    from {{ref('MARA')}}
)
SELECT
   mch1.material_number AS MaterialId,
   mch1.batch_number AS BatchId,
   CAST(mch1.manufacture_date AS DATE) as manufacture_date,
   CAST(mch1.shelf_life_expiration_date AS DATE) as shelf_life_expiration_date,
   -- Calculated ship life expiration date by subtracting minimum remaining shelf life from shelf life expiration date
   (mch1.shelf_life_expiration_date - (mara.minimum_remaining_shelf_life || ' days')::INTERVAL) AS ship_life_expiration_date
FROM
   MCH1 AS mch1
   LEFT JOIN
   (SELECT * FROM MARA WHERE '__END_AT' IS NULL) mara
   ON mch1.material_number = mara.material_number