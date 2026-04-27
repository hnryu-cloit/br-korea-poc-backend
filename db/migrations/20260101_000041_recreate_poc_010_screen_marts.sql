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

CREATE TABLE IF NOT EXISTS mart_poc_010_analytics_daily (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    sale_dt VARCHAR(8) NOT NULL,
    total_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    coffee_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    online_total_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    online_delivery_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    online_pickup_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    offline_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    delivery_order_count NUMERIC(18,2) NOT NULL DEFAULT 0,
    pickup_order_count NUMERIC(18,2) NOT NULL DEFAULT 0,
    hall_visit_order_count NUMERIC(18,2) NOT NULL DEFAULT 0,
    online_order_count NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_order_count NUMERIC(18,2) NOT NULL DEFAULT 0,
    discount_payment_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    payment_total_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sale_dt)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_analytics_daily_sale_dt
    ON mart_poc_010_analytics_daily(sale_dt DESC);

CREATE TABLE IF NOT EXISTS mart_poc_010_analytics_hourly (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    sale_dt VARCHAR(8) NOT NULL,
    hour INTEGER NOT NULL,
    total_sales_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sale_dt, hour)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_analytics_hourly_sale_dt
    ON mart_poc_010_analytics_hourly(sale_dt DESC, hour ASC);

CREATE TABLE IF NOT EXISTS mart_poc_010_analytics_deadline (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    deadline_at TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deadline_at)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_analytics_deadline_at
    ON mart_poc_010_analytics_deadline(deadline_at ASC);

CREATE TABLE IF NOT EXISTS mart_poc_010_production_inventory_status (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    business_date VARCHAR(8) NOT NULL,
    item_cd VARCHAR(128) NOT NULL,
    item_nm VARCHAR(255) NOT NULL,
    total_stock NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_sold NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_orderable NUMERIC(18,2) NOT NULL DEFAULT 0,
    stock_rate NUMERIC(18,4) NOT NULL DEFAULT 0,
    stockout_hour INTEGER NULL,
    is_stockout BOOLEAN NOT NULL DEFAULT FALSE,
    assumed_shelf_life_days INTEGER NOT NULL DEFAULT 1,
    expiry_risk_level VARCHAR(16) NOT NULL DEFAULT 'low',
    status VARCHAR(16) NOT NULL DEFAULT 'normal',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_date, item_cd)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_production_inventory_status_lookup
    ON mart_poc_010_production_inventory_status(business_date DESC, stock_rate ASC, item_nm ASC);

CREATE TABLE IF NOT EXISTS mart_poc_010_production_waste_daily (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    target_date VARCHAR(8) NOT NULL,
    item_cd VARCHAR(128) NOT NULL,
    item_nm VARCHAR(255) NOT NULL,
    total_waste_qty NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_waste_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    avg_cost NUMERIC(18,2) NOT NULL DEFAULT 0,
    adjusted_loss_qty NUMERIC(18,2) NOT NULL DEFAULT 0,
    adjusted_loss_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    estimated_expiry_loss_qty NUMERIC(18,2) NOT NULL DEFAULT 0,
    assumed_shelf_life_days INTEGER NOT NULL DEFAULT 1,
    expiry_risk_level VARCHAR(16) NOT NULL DEFAULT 'low',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (target_date, item_cd, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_production_waste_daily_target_date
    ON mart_poc_010_production_waste_daily(target_date DESC, total_waste_amount DESC, total_waste_qty DESC);

CREATE TABLE IF NOT EXISTS mart_poc_010_production_waste_monthly (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    target_month VARCHAR(7) NOT NULL,
    item_cd VARCHAR(128) NOT NULL,
    item_nm VARCHAR(255) NOT NULL,
    total_waste_qty NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_waste_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    avg_cost NUMERIC(18,2) NOT NULL DEFAULT 0,
    adjusted_loss_qty NUMERIC(18,2) NOT NULL DEFAULT 0,
    adjusted_loss_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    estimated_expiry_loss_qty NUMERIC(18,2) NOT NULL DEFAULT 0,
    assumed_shelf_life_days INTEGER NOT NULL DEFAULT 1,
    expiry_risk_level VARCHAR(16) NOT NULL DEFAULT 'low',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (target_month, item_cd, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_production_waste_monthly_target_month
    ON mart_poc_010_production_waste_monthly(target_month DESC, total_waste_qty DESC);

CREATE TABLE IF NOT EXISTS mart_sales_margin_daily (
    store_id VARCHAR(64) NOT NULL,
    target_date VARCHAR(8) NOT NULL,
    window_start_date VARCHAR(8) NOT NULL,
    window_end_date VARCHAR(8) NOT NULL,
    avg_margin_rate NUMERIC(12, 6) NOT NULL DEFAULT 0,
    avg_net_profit_per_item NUMERIC(12, 2) NOT NULL DEFAULT 0,
    product_count INTEGER NOT NULL DEFAULT 0,
    generated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, target_date)
);

CREATE INDEX IF NOT EXISTS idx_mart_sales_margin_daily_target_date
    ON mart_sales_margin_daily (target_date);
