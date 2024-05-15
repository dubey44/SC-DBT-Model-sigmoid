{{ config(materialized='table') }}

with mchb as
(
    select * 
    from {{ref('MCHB')}}
),
mchbh as(
    select *
    from {{ref('MCHBH')}}
),
mbew as(
    select *
    from {{ref('MBEW')}}
),
mbewh as(
    select *
    from {{ref('MBEWH')}}
),
mara as(
    select *
    from {{ref('MARA')}}
)

