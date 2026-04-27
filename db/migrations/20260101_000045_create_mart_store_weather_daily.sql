CREATE TABLE IF NOT EXISTS mart_store_weather_daily (
    store_id VARCHAR(64) NOT NULL,
    store_name TEXT,
    sido TEXT,
    weather_dt VARCHAR(8) NOT NULL,
    weather_type TEXT,
    avg_temp_c NUMERIC(8, 2),
    max_temp_c NUMERIC(8, 2),
    min_temp_c NUMERIC(8, 2),
    precipitation_mm NUMERIC(10, 2),
    precipitation_probability_max NUMERIC(6, 2),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, weather_dt)
);

CREATE INDEX IF NOT EXISTS idx_mart_store_weather_daily_lookup
    ON mart_store_weather_daily (store_id, weather_dt DESC);

CREATE INDEX IF NOT EXISTS idx_mart_store_weather_daily_weather_dt
    ON mart_store_weather_daily (weather_dt DESC);
