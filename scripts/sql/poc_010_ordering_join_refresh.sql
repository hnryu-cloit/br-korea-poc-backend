DELETE FROM mart_ordering_join_poc_010
WHERE store_id = :store_id
  AND dlv_dt BETWEEN :start_date AND :end_date;

INSERT INTO mart_ordering_join_poc_010 (
    store_id,
    store_name,
    sido,
    sigungu,
    business_date,
    dlv_dt,
    weather_date,
    weather_region,
    weather_type,
    weather_max_temperature_c,
    weather_min_temperature_c,
    weather_precipitation_probability,
    item_cd,
    item_nm,
    ord_qty,
    confrm_qty,
    ord_rec_qty,
    auto_ord_yn,
    ord_grp_nm,
    generated_at,
    updated_at
)
WITH grouped_orders AS (
    SELECT
        REPLACE(CAST(o.dlv_dt AS TEXT), '-', '') AS business_date,
        REPLACE(CAST(o.dlv_dt AS TEXT), '-', '') AS dlv_dt,
        CAST(o.item_nm AS TEXT) AS item_nm,
        MIN(CAST(o.item_cd AS TEXT)) AS item_cd,
        ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(o.ord_qty AS TEXT)), '')::numeric, 0))) AS ord_qty,
        ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(o.confrm_qty AS TEXT)), '')::numeric, 0))) AS confrm_qty,
        ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(o.ord_rec_qty AS TEXT)), '')::numeric, 0))) AS ord_rec_qty,
        MAX(CAST(o.auto_ord_yn AS TEXT)) AS auto_ord_yn,
        MAX(CAST(o.ord_grp_nm AS TEXT)) AS ord_grp_nm
    FROM raw_order_extract o
    WHERE CAST(o.masked_stor_cd AS TEXT) = :store_id
      AND REPLACE(CAST(o.dlv_dt AS TEXT), '-', '') BETWEEN :start_date AND :end_date
    GROUP BY
        REPLACE(CAST(o.dlv_dt AS TEXT), '-', ''),
        CAST(o.item_nm AS TEXT)
)
SELECT
    CAST(:store_id AS VARCHAR(64)) AS store_id,
    CAST(sm.maked_stor_nm AS TEXT) AS store_name,
    CAST(sm.sido AS TEXT) AS sido,
    CAST(sm.region AS TEXT) AS sigungu,
    o.business_date,
    o.dlv_dt,
    REPLACE(CAST(w.weather_dt AS TEXT), '-', '') AS weather_date,
    CAST(w.sido AS TEXT) AS weather_region,
    COALESCE(CAST(w.weather_type AS TEXT), '맑음') AS weather_type,
    CAST(w.max_temp_c AS INTEGER) AS weather_max_temperature_c,
    CAST(w.min_temp_c AS INTEGER) AS weather_min_temperature_c,
    CAST(w.precipitation_probability_max AS INTEGER) AS weather_precipitation_probability,
    o.item_cd,
    o.item_nm,
    o.ord_qty,
    o.confrm_qty,
    o.ord_rec_qty,
    o.auto_ord_yn,
    o.ord_grp_nm,
    NOW() AS generated_at,
    NOW() AS updated_at
FROM grouped_orders o
LEFT JOIN raw_store_master sm
  ON sm.masked_stor_cd = :store_id
LEFT JOIN mart_store_weather_daily w
  ON w.store_id = :store_id
 AND REPLACE(CAST(w.weather_dt AS TEXT), '-', '') = o.dlv_dt
GROUP BY
    o.business_date,
    o.dlv_dt,
    REPLACE(CAST(w.weather_dt AS TEXT), '-', ''),
    CAST(w.sido AS TEXT),
    CAST(w.weather_type AS TEXT),
    w.max_temp_c,
    w.min_temp_c,
    w.precipitation_probability_max,
    CAST(sm.maked_stor_nm AS TEXT),
    CAST(sm.sido AS TEXT),
    CAST(sm.region AS TEXT),
    o.item_cd,
    o.item_nm,
    o.ord_qty,
    o.confrm_qty,
    o.ord_rec_qty,
    o.auto_ord_yn,
    o.ord_grp_nm;
