ALTER TABLE raw_weather_daily
    ADD COLUMN IF NOT EXISTS max_temp_c NUMERIC(8, 2);

ALTER TABLE raw_weather_daily
    ADD COLUMN IF NOT EXISTS min_temp_c NUMERIC(8, 2);

ALTER TABLE raw_weather_daily
    ADD COLUMN IF NOT EXISTS precipitation_probability_max INTEGER;
