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
