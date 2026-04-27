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
