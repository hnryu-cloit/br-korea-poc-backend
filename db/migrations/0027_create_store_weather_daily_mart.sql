CREATE TABLE IF NOT EXISTS mart_store_weather_daily (
    store_id VARCHAR(64) NOT NULL,
    store_name TEXT,
    sido TEXT NOT NULL,
    weather_dt TEXT NOT NULL,
    weather_type TEXT NOT NULL,
    avg_temp_c NUMERIC(8,2) NOT NULL DEFAULT 0,
    max_temp_c NUMERIC(8,2),
    min_temp_c NUMERIC(8,2),
    precipitation_mm NUMERIC(10,2) NOT NULL DEFAULT 0,
    precipitation_probability_max INTEGER,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, weather_dt)
);

CREATE INDEX IF NOT EXISTS idx_mart_store_weather_daily_dt
    ON mart_store_weather_daily(weather_dt ASC);

CREATE INDEX IF NOT EXISTS idx_mart_store_weather_daily_sido
    ON mart_store_weather_daily(sido ASC);
