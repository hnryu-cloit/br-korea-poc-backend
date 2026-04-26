DELETE FROM mart_poc_010_analytics_daily
WHERE sale_dt BETWEEN :start_date AND :end_date;

INSERT INTO mart_poc_010_analytics_daily (
    store_id,
    sale_dt,
    total_sales_amount,
    coffee_sales_amount,
    online_total_sales_amount,
    online_delivery_sales_amount,
    online_pickup_sales_amount,
    offline_sales_amount,
    delivery_order_count,
    pickup_order_count,
    hall_visit_order_count,
    online_order_count,
    total_order_count,
    discount_payment_amount,
    payment_total_amount,
    generated_at,
    updated_at
)
WITH item_sales AS (
    SELECT
        masked_stor_cd AS store_id,
        sale_dt,
        SUM(COALESCE(NULLIF(TRIM(CAST(sale_amt AS TEXT)), '')::numeric, 0)) AS total_sales_amount
    FROM raw_daily_store_item
    WHERE masked_stor_cd = :store_id
      AND sale_dt BETWEEN :start_date AND :end_date
    GROUP BY masked_stor_cd, sale_dt
),
channel_sales AS (
    SELECT
        masked_stor_cd AS store_id,
        sale_dt,
        SUM(COALESCE(NULLIF(TRIM(CAST(sale_amt AS TEXT)), '')::numeric, 0)) AS channel_total_sales_amount,
        SUM(CASE WHEN COALESCE(ho_chnl_div, '') LIKE '%배달%' THEN COALESCE(NULLIF(TRIM(CAST(sale_amt AS TEXT)), '')::numeric, 0) ELSE 0 END) AS online_delivery_sales_amount,
        SUM(CASE WHEN COALESCE(ho_chnl_div, '') LIKE '%픽업%' OR COALESCE(ho_chnl_div, '') LIKE '%포장%' THEN COALESCE(NULLIF(TRIM(CAST(sale_amt AS TEXT)), '')::numeric, 0) ELSE 0 END) AS online_pickup_sales_amount,
        SUM(CASE WHEN COALESCE(ho_chnl_div, '') LIKE '%배달%' THEN COALESCE(NULLIF(TRIM(CAST(ord_cnt AS TEXT)), '')::numeric, 0) ELSE 0 END) AS delivery_order_count,
        SUM(CASE WHEN COALESCE(ho_chnl_div, '') LIKE '%픽업%' OR COALESCE(ho_chnl_div, '') LIKE '%포장%' THEN COALESCE(NULLIF(TRIM(CAST(ord_cnt AS TEXT)), '')::numeric, 0) ELSE 0 END) AS pickup_order_count,
        SUM(COALESCE(NULLIF(TRIM(CAST(ord_cnt AS TEXT)), '')::numeric, 0)) AS total_order_count
    FROM raw_daily_store_channel
    WHERE masked_stor_cd = :store_id
      AND sale_dt BETWEEN :start_date AND :end_date
    GROUP BY masked_stor_cd, sale_dt
),
coffee_sales AS (
    SELECT
        s.masked_stor_cd AS store_id,
        s.sale_dt,
        SUM(COALESCE(NULLIF(TRIM(CAST(s.sale_amt AS TEXT)), '')::numeric, 0)) AS coffee_sales_amount
    FROM core_daily_item_sales s
    JOIN mart_item_category_master c
      ON (
            COALESCE(NULLIF(TRIM(CAST(c.item_cd AS TEXT)), ''), '') <> ''
        AND COALESCE(NULLIF(TRIM(CAST(c.item_cd AS TEXT)), ''), '') = COALESCE(NULLIF(TRIM(CAST(s.item_cd AS TEXT)), ''), '')
      )
       OR COALESCE(NULLIF(TRIM(CAST(c.item_nm AS TEXT)), ''), '') = COALESCE(NULLIF(TRIM(CAST(s.item_nm AS TEXT)), ''), '')
    WHERE s.masked_stor_cd = :store_id
      AND s.sale_dt BETWEEN :start_date AND :end_date
      AND COALESCE(c.is_coffee, FALSE) = TRUE
    GROUP BY s.masked_stor_cd, s.sale_dt
)
SELECT
    CAST(:store_id AS VARCHAR(64)) AS store_id,
    i.sale_dt,
    COALESCE(i.total_sales_amount, 0) AS total_sales_amount,
    COALESCE(cf.coffee_sales_amount, 0) AS coffee_sales_amount,
    COALESCE(ch.online_delivery_sales_amount, 0) + COALESCE(ch.online_pickup_sales_amount, 0) AS online_total_sales_amount,
    COALESCE(ch.online_delivery_sales_amount, 0) AS online_delivery_sales_amount,
    COALESCE(ch.online_pickup_sales_amount, 0) AS online_pickup_sales_amount,
    GREATEST(
        COALESCE(i.total_sales_amount, 0)
        - (COALESCE(ch.online_delivery_sales_amount, 0) + COALESCE(ch.online_pickup_sales_amount, 0)),
        0
    ) AS offline_sales_amount,
    COALESCE(ch.delivery_order_count, 0) AS delivery_order_count,
    COALESCE(ch.pickup_order_count, 0) AS pickup_order_count,
    GREATEST(COALESCE(ch.total_order_count, 0) - (COALESCE(ch.delivery_order_count, 0) + COALESCE(ch.pickup_order_count, 0)), 0) AS hall_visit_order_count,
    COALESCE(ch.delivery_order_count, 0) + COALESCE(ch.pickup_order_count, 0) AS online_order_count,
    COALESCE(ch.total_order_count, 0) AS total_order_count,
    0 AS discount_payment_amount,
    COALESCE(i.total_sales_amount, 0) AS payment_total_amount,
    NOW(),
    NOW()
FROM item_sales i
LEFT JOIN channel_sales ch
  ON ch.store_id = i.store_id
 AND ch.sale_dt = i.sale_dt
LEFT JOIN coffee_sales cf
  ON cf.store_id = i.store_id
 AND cf.sale_dt = i.sale_dt
ORDER BY i.sale_dt;
