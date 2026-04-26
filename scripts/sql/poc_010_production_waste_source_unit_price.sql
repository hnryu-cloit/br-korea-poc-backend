SELECT
    COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')) AS item_cd,
    COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')) AS item_nm,
    AVG(actual_sale_amt / NULLIF(sale_qty, 0)) AS avg_unit_price
FROM core_daily_item_sales
WHERE masked_stor_cd = :store_id
  AND sale_dt BETWEEN :lookback_start AND :end_date
  AND sale_qty > 0
GROUP BY
    COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')),
    COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), ''))
ORDER BY item_nm;
