INSERT INTO mart_sales_margin_daily (
    store_id,
    target_date,
    window_start_date,
    window_end_date,
    avg_margin_rate,
    avg_net_profit_per_item,
    product_count,
    generated_at,
    updated_at
)
WITH sold_products AS (
    SELECT DISTINCT COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '') AS item_nm
    FROM raw_daily_store_item
    WHERE masked_stor_cd = :store_id
      AND sale_dt >= :window_start
      AND sale_dt <= :target_date
      AND COALESCE(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC), 0) > 0
),
product_margin AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '') AS item_nm,
        AVG(
            (CAST(p.sale_prc AS NUMERIC) - CAST(p.item_cost AS NUMERIC))
            / NULLIF(CAST(p.sale_prc AS NUMERIC), 0)
        ) AS margin_rate,
        AVG(CAST(p.sale_prc AS NUMERIC) - CAST(p.item_cost AS NUMERIC)) AS net_profit_per_item
    FROM raw_production_extract p
    JOIN sold_products s
      ON s.item_nm = COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '')
    WHERE p.masked_stor_cd = :store_id
      AND p.prod_dt >= :window_start
      AND p.prod_dt <= :target_date
      AND CAST(p.sale_prc AS NUMERIC) > 0
      AND CAST(p.item_cost AS NUMERIC) > 0
    GROUP BY COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '')
)
SELECT
    CAST(:store_id AS VARCHAR(64)),
    :target_date,
    :window_start,
    :target_date,
    COALESCE(AVG(margin_rate), 0),
    COALESCE(AVG(net_profit_per_item), 0),
    COUNT(*),
    NOW(),
    NOW()
FROM product_margin
ON CONFLICT (store_id, target_date)
DO UPDATE SET
    window_start_date = EXCLUDED.window_start_date,
    window_end_date = EXCLUDED.window_end_date,
    avg_margin_rate = EXCLUDED.avg_margin_rate,
    avg_net_profit_per_item = EXCLUDED.avg_net_profit_per_item,
    product_count = EXCLUDED.product_count,
    generated_at = EXCLUDED.generated_at,
    updated_at = NOW();
