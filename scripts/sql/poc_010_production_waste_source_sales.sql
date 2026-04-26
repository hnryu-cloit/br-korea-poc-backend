SELECT
    s.sale_dt,
    COALESCE(NULLIF(TRIM(s.item_cd), ''), NULLIF(TRIM(s.item_nm), '')) AS item_cd,
    COALESCE(NULLIF(TRIM(s.item_nm), ''), NULLIF(TRIM(s.item_cd), '')) AS item_nm,
    SUM(COALESCE(s.sale_qty, 0)) AS sale_qty
FROM core_daily_item_sales s
WHERE s.masked_stor_cd = :store_id
  AND s.sale_dt BETWEEN :lookback_start AND :end_date
GROUP BY
    s.sale_dt,
    COALESCE(NULLIF(TRIM(s.item_cd), ''), NULLIF(TRIM(s.item_nm), '')),
    COALESCE(NULLIF(TRIM(s.item_nm), ''), NULLIF(TRIM(s.item_cd), ''))
HAVING SUM(COALESCE(s.sale_qty, 0)) > 0
ORDER BY s.sale_dt, item_nm;
