{{ config(materialized='incremental') }}
SELECT
       A.Date,
       A.PlantID,
       A.StorageLocationId,
       A.MaterialId,
       A.BatchId,
       CAST(A.PostingDate AS DATE) AS LastPostingDate
   FROM
       (
           -- Subquery to rank PostingDate for each unique combination
           SELECT
               A.Date,
               A.Calander_Date,
               A.PlantId,
               A.StorageLocationID,
               A.MaterialId,
               A.BatchId,
               B.PostingDate,
               RANK() OVER (PARTITION BY A.Date, A.PlantId, A.StorageLocationID, A.MaterialId, A.BatchId ORDER BY B.PostingDate DESC) AS RN
           FROM
               {{ref('batch_master')}} A
               LEFT JOIN (                   
                   SELECT
 MaterialId,
                       PlantId,
                       StorageLocationId,
                       PostingDate,
                       BatchId
                   FROM
                       {{ref('stock_movement')}}
                   GROUP BY
                       MaterialId,
                       PlantId,
                       StorageLocationId,
                       PostingDate,
                       BatchId
               ) B ON A.MaterialId = B.MaterialID
                   AND A.PlantID = B.PlantID
                   AND A.StorageLocationID = B.StorageLocationID
                   AND A.BatchId = B.BatchId
                   AND A.Calander_Date > B.PostingDate
       ) A
   WHERE
       RN = 1 -- Selecting only the latest posting date for each unique combination
