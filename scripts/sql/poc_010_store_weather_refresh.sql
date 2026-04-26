DELETE FROM mart_store_weather_daily
WHERE store_id = :store_id
  AND REPLACE(CAST(weather_dt AS TEXT), '-', '') BETWEEN :start_date AND :end_date;

INSERT INTO mart_store_weather_daily (
    store_id,
    store_name,
    sido,
    weather_dt,
    weather_type,
    avg_temp_c,
    max_temp_c,
    min_temp_c,
    precipitation_mm,
    precipitation_probability_max,
    generated_at,
    updated_at
)
SELECT
    CAST(:store_id AS VARCHAR(64)) AS store_id,
    NULLIF(TRIM(CAST(s.maked_stor_nm AS TEXT)), '') AS store_name,
    CAST(w.sido AS TEXT) AS sido,
    REPLACE(CAST(w.weather_dt AS TEXT), '-', '') AS weather_dt,
    CASE
        WHEN COALESCE(w.precipitation_mm, 0) >= 5 THEN CASE WHEN COALESCE(w.avg_temp_c, 0) <= 0 THEN '눈' ELSE '비' END
        WHEN COALESCE(w.precipitation_mm, 0) > 0 THEN CASE WHEN COALESCE(w.avg_temp_c, 0) <= 1 THEN '진눈깨비' ELSE '흐리고 비' END
        WHEN COALESCE(w.avg_temp_c, 0) <= 0 THEN '흐림'
        ELSE '맑음'
    END AS weather_type,
    COALESCE(w.avg_temp_c, 0) AS avg_temp_c,
    NULL::NUMERIC AS max_temp_c,
    NULL::NUMERIC AS min_temp_c,
    COALESCE(w.precipitation_mm, 0) AS precipitation_mm,
    NULL::NUMERIC AS precipitation_probability_max,
    NOW(),
    NOW()
FROM raw_store_master s
JOIN raw_weather_daily w
  ON CAST(w.sido AS TEXT) = CAST(s.sido AS TEXT)
WHERE s.masked_stor_cd = :store_id
  AND REPLACE(CAST(w.weather_dt AS TEXT), '-', '') BETWEEN :start_date AND :end_date;
