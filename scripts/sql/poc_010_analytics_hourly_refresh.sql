DELETE FROM mart_poc_010_analytics_hourly
WHERE sale_dt BETWEEN :start_date AND :end_date;

INSERT INTO mart_poc_010_analytics_hourly (
    store_id,
    sale_dt,
    hour,
    total_sales_amount,
    generated_at,
    updated_at
)
SELECT
    CAST(:store_id AS VARCHAR(64)) AS store_id,
    sale_dt,
    CAST(tmzon_div AS INTEGER) AS hour,
    SUM(COALESCE(NULLIF(TRIM(CAST(sale_amt AS TEXT)), '')::numeric, 0)) AS total_sales_amount,
    NOW(),
    NOW()
FROM raw_daily_store_item_tmzon
WHERE masked_stor_cd = :store_id
  AND sale_dt BETWEEN :start_date AND :end_date
  AND COALESCE(NULLIF(TRIM(CAST(tmzon_div AS TEXT)), ''), '') ~ '^[0-9]+$'
GROUP BY sale_dt, CAST(tmzon_div AS INTEGER)
ORDER BY sale_dt, hour;
