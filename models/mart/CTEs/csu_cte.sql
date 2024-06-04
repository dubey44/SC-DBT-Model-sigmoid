with vbap as(
    select 
    material_number as matnr,
    sales_document as vbeln,
    cumulative_order_quantity as kwmeng,
    factor_for_converting_1 as umziz,
    factor_for_converting_2 as umzin,
    target_quantity_uom as zieme,
    base_uom as meins,

    * from {{ref('VBAP')}}
),
marm as(
    select material_number as matnr,
    base_uom_conversion_denominator as umren,
    base_uom_conversion_numerator as umrez,
    stockkeeping_unit_alternative_uom as meinh,
    * from {{ref('MARM')}}
)

select order_no, sum(quantity_csu) as csu_quantity
from
(   select vbap.vbeln as order_no,
    vbap.matnr,
    vbap.kwmeng as quantity,
    (vbap.kwmeng * (vbap.umziz/vbap.umzin)) as quantity_buom,
    ((vbap.kwmeng * (vbap.umziz/vbap.umzin)) * (marm.umren/marm.umrez)) as quantity_csu,
    vbap.zieme as uom,
    vbap.umziz as numerator,
    vbap.umzin as denominator,
    vbap.meins as buom,
    marm.meinh as alt_uom,
    marm.umrez as alt_num,
    marm.umren as alt_denom
    from vbap
    left join marm
    on vbap.matnr = marm.matnr
    where marm.meinh = 'csu'
)

group by order_no



-- SELECT ORDER_NO, SUM(QUANTITY_CSU) AS CSU_QUANTITY
-- FROM
-- (   SELECT VBAP.VBELN AS ORDER_NO,
--     VBAP.MATNR,
--     VBAP.KWMENG AS QUANTITY,
--     (VBAP.KWMENG * (VBAP.UMZIZ/VBAP.UMZIN)) AS QUANTITY_BUOM,
--     ((VBAP.KWMENG * (VBAP.UMZIZ/VBAP.UMZIN)) * (MARM.UMREN/MARM.UMREZ)) AS QUANTITY_CSU,
--     VBAP.ZIEME AS UOM,
--     VBAP.UMZIZ AS NUMERATOR,
--     VBAP.UMZIN AS DENOMINATOR,
--     VBAP.MEINS AS BUOM,
--     MARM.MEINH AS ALT_UOM,
--     MARM.UMREZ AS ALT_NUM,
--     MARM.UMREN AS ALT_DENOM
--     FROM VBAP
--     LEFT JOIN snowflake_prd_landzone.RAW_SAP.TBL_SAP_MARM AS MARM
--     ON VBAP.MATNR = MARM.MATNR
--     WHERE MARM.MEINH = 'CSU'
-- )

-- GROUP BY ORDER_NO
