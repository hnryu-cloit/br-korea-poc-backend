DELETE FROM mart_poc_010_production_inventory_status
WHERE business_date BETWEEN :start_date AND :end_date;

INSERT INTO mart_poc_010_production_inventory_status (
    store_id,
    business_date,
    item_cd,
    item_nm,
    total_stock,
    total_sold,
    total_orderable,
    stock_rate,
    stockout_hour,
    is_stockout,
    assumed_shelf_life_days,
    expiry_risk_level,
    status,
    generated_at,
    updated_at
)
WITH aggregated AS (
    SELECT
        stock_dt AS business_date,
        COALESCE(NULLIF(TRIM(CAST(item_cd AS TEXT)), ''), NULLIF(TRIM(CAST(item_nm AS TEXT)), '')) AS item_cd,
        COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), NULLIF(TRIM(CAST(item_cd AS TEXT)), '')) AS item_nm,
        SUM(COALESCE(NULLIF(TRIM(CAST(stock_qty AS TEXT)), '')::numeric, 0)) AS total_stock,
        SUM(COALESCE(NULLIF(TRIM(CAST(sale_qty AS TEXT)), '')::numeric, 0)) AS total_sold
    FROM raw_inventory_extract
    WHERE masked_stor_cd = :store_id
      AND stock_dt BETWEEN :start_date AND :end_date
    GROUP BY
        stock_dt,
        COALESCE(NULLIF(TRIM(CAST(item_cd AS TEXT)), ''), NULLIF(TRIM(CAST(item_nm AS TEXT)), '')),
        COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), NULLIF(TRIM(CAST(item_cd AS TEXT)), ''))
)
SELECT
    CAST(:store_id AS VARCHAR(64)) AS store_id,
    a.business_date,
    a.item_cd,
    a.item_nm,
    a.total_stock,
    a.total_sold,
    GREATEST(a.total_stock + a.total_sold, 0) AS total_orderable,
    CASE
        WHEN a.total_sold > 0 THEN ROUND(a.total_stock / NULLIF(a.total_sold, 0), 4)
        WHEN a.total_stock < 0 THEN -1
        WHEN a.total_stock > 0 THEN 1
        ELSE 0
    END AS stock_rate,
    NULL AS stockout_hour,
    CASE WHEN a.total_stock < 0 THEN TRUE ELSE FALSE END AS is_stockout,
    COALESCE(NULLIF(TRIM(CAST(sl.shelf_life_days AS TEXT)), '')::INT, 1) AS assumed_shelf_life_days,
    CASE
        WHEN COALESCE(NULLIF(TRIM(CAST(sl.shelf_life_days AS TEXT)), '')::INT, 1) <= 1 AND (CASE WHEN a.total_sold > 0 THEN a.total_stock / NULLIF(a.total_sold, 0) ELSE 0 END) > 0.25 THEN 'high'
        WHEN (CASE WHEN a.total_sold > 0 THEN a.total_stock / NULLIF(a.total_sold, 0) ELSE 0 END) > 0.15 THEN 'medium'
        ELSE 'low'
    END AS expiry_risk_level,
    CASE
        WHEN a.total_stock < 0 THEN 'shortage'
        WHEN (CASE WHEN a.total_sold > 0 THEN a.total_stock / NULLIF(a.total_sold, 0) ELSE 0 END) >= 0.35 THEN 'excess'
        ELSE 'normal'
    END AS status,
    NOW(),
    NOW()
FROM aggregated a
LEFT JOIN raw_product_shelf_life sl
  ON COALESCE(NULLIF(TRIM(CAST(sl.item_cd AS TEXT)), ''), NULLIF(TRIM(CAST(sl.item_nm AS TEXT)), '')) =
     a.item_cd;
