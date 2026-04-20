CREATE TABLE IF NOT EXISTS raw_weather_daily (
    weather_dt TEXT NOT NULL,
    sido TEXT NOT NULL,
    avg_temp_c NUMERIC(8, 2) NOT NULL,
    precipitation_mm NUMERIC(10, 2) NOT NULL,
    source_provider TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (weather_dt, sido)
);

CREATE INDEX IF NOT EXISTS idx_raw_weather_daily_sido_date
    ON raw_weather_daily(sido, weather_dt);
