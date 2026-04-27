CREATE TABLE IF NOT EXISTS mart_production_status (
    store_id VARCHAR(64) NOT NULL,
    business_date VARCHAR(8) NOT NULL,
    reference_hour INTEGER NOT NULL,
    sku_id VARCHAR(128) NOT NULL,
    sku_name TEXT NOT NULL,
    image_url TEXT,
    current_stock_qty INTEGER NOT NULL DEFAULT 0,
    forecast_stock_1h_qty INTEGER NOT NULL DEFAULT 0,
    predicted_sales_1h_qty INTEGER NOT NULL DEFAULT 0,
    forecast_baseline_qty INTEGER NOT NULL DEFAULT 0,
    avg_first_production_qty_4w INTEGER NOT NULL DEFAULT 0,
    avg_first_production_time_4w VARCHAR(16) NOT NULL DEFAULT '00:00',
    avg_second_production_qty_4w INTEGER NOT NULL DEFAULT 0,
    avg_second_production_time_4w VARCHAR(16) NOT NULL DEFAULT '00:00',
    recommended_production_qty INTEGER NOT NULL DEFAULT 0,
    order_confirm_qty INTEGER NOT NULL DEFAULT 0,
    hourly_sale_qty INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'safe',
    risk_level_label VARCHAR(32) NOT NULL DEFAULT '정상',
    depletion_time VARCHAR(16) NOT NULL DEFAULT '-',
    depletion_eta_minutes INTEGER,
    predicted_stockout_time VARCHAR(32),
    sales_velocity NUMERIC(12,4),
    chance_loss_saving_pct INTEGER NOT NULL DEFAULT 0,
    chance_loss_basis_text TEXT,
    alert_message TEXT,
    speed_alert BOOLEAN NOT NULL DEFAULT FALSE,
    speed_alert_message TEXT,
    material_alert BOOLEAN NOT NULL DEFAULT FALSE,
    material_alert_message TEXT,
    can_produce BOOLEAN NOT NULL DEFAULT TRUE,
    decision_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_snapshot_id BIGINT REFERENCES production_prediction_snapshots(id) ON DELETE SET NULL,
    source_mode VARCHAR(32) NOT NULL DEFAULT 'snapshot',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, business_date, reference_hour, sku_id)
);

CREATE INDEX IF NOT EXISTS idx_mart_production_status_lookup
    ON mart_production_status(store_id, business_date, reference_hour DESC, status);

CREATE INDEX IF NOT EXISTS idx_mart_production_status_snapshot
    ON mart_production_status(source_snapshot_id);
