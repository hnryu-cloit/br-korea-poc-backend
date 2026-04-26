SELECT
    p.prod_dt,
    COALESCE(NULLIF(TRIM(p.item_cd), ''), NULLIF(TRIM(p.item_nm), '')) AS item_cd,
    COALESCE(NULLIF(TRIM(p.item_nm), ''), NULLIF(TRIM(p.item_cd), '')) AS item_nm,
    SUM(
        COALESCE(NULLIF(TRIM(p.prod_qty), '')::numeric, 0)
        + COALESCE(NULLIF(TRIM(p.prod_qty_2), '')::numeric, 0)
        + COALESCE(NULLIF(TRIM(p.prod_qty_3), '')::numeric, 0)
        + COALESCE(NULLIF(TRIM(p.reprod_qty), '')::numeric, 0)
    ) AS produced_qty
FROM raw_production_extract p
WHERE p.masked_stor_cd = :store_id
  AND p.prod_dt BETWEEN :lookback_start AND :end_date
GROUP BY
    p.prod_dt,
    COALESCE(NULLIF(TRIM(p.item_cd), ''), NULLIF(TRIM(p.item_nm), '')),
    COALESCE(NULLIF(TRIM(p.item_nm), ''), NULLIF(TRIM(p.item_cd), ''))
HAVING SUM(
    COALESCE(NULLIF(TRIM(p.prod_qty), '')::numeric, 0)
    + COALESCE(NULLIF(TRIM(p.prod_qty_2), '')::numeric, 0)
    + COALESCE(NULLIF(TRIM(p.prod_qty_3), '')::numeric, 0)
    + COALESCE(NULLIF(TRIM(p.reprod_qty), '')::numeric, 0)
) > 0
ORDER BY p.prod_dt, item_nm;
