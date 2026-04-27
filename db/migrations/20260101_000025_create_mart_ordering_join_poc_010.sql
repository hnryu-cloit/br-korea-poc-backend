CREATE TABLE IF NOT EXISTS mart_ordering_join_poc_010 (
    store_id VARCHAR(64) NOT NULL,
    store_name TEXT,
    sido TEXT,
    sigungu TEXT,
    business_date VARCHAR(8) NOT NULL,
    dlv_dt VARCHAR(8) NOT NULL,
    weather_date VARCHAR(8),
    weather_region TEXT,
    weather_type TEXT,
    weather_max_temperature_c INTEGER,
    weather_min_temperature_c INTEGER,
    weather_precipitation_probability INTEGER,
    item_cd VARCHAR(128),
    item_nm TEXT NOT NULL,
    ord_qty NUMERIC(14,2),
    confrm_qty NUMERIC(14,2),
    ord_rec_qty NUMERIC(14,2),
    auto_ord_yn VARCHAR(16),
    ord_grp_nm TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, dlv_dt, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_ordering_join_poc_010_lookup
    ON mart_ordering_join_poc_010(store_id, business_date, dlv_dt, item_nm);
