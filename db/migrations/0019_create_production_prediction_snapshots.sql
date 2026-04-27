CREATE TABLE IF NOT EXISTS production_prediction_snapshots (
    id BIGSERIAL PRIMARY KEY,
    store_id VARCHAR(64) NOT NULL,
    business_date VARCHAR(8) NOT NULL,
    target_hour INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    source_snapshot_hour INTEGER,
    model_version VARCHAR(128),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    fallback_allowed_until TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_production_prediction_snapshots UNIQUE (store_id, business_date, target_hour)
);

CREATE TABLE IF NOT EXISTS production_prediction_snapshot_items (
    snapshot_id BIGINT NOT NULL REFERENCES production_prediction_snapshots(id) ON DELETE CASCADE,
    sku_id VARCHAR(128) NOT NULL,
    name TEXT NOT NULL,
    current_stock INTEGER NOT NULL DEFAULT 0,
    predicted_stock_1h INTEGER NOT NULL DEFAULT 0,
    predicted_sales_1h INTEGER NOT NULL DEFAULT 0,
    forecast_baseline INTEGER NOT NULL DEFAULT 0,
    recommended_production_qty INTEGER NOT NULL DEFAULT 0,
    avg_first_production_qty_4w INTEGER NOT NULL DEFAULT 0,
    avg_first_production_time_4w VARCHAR(16) NOT NULL DEFAULT '00:00',
    avg_second_production_qty_4w INTEGER NOT NULL DEFAULT 0,
    avg_second_production_time_4w VARCHAR(16) NOT NULL DEFAULT '00:00',
    order_confirm_qty INTEGER NOT NULL DEFAULT 0,
    hourly_sale_qty INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'safe',
    depletion_time VARCHAR(16) NOT NULL DEFAULT '-',
    stockout_expected_at VARCHAR(32),
    alert_message TEXT,
    confidence NUMERIC(6,4) NOT NULL DEFAULT 0,
    chance_loss_qty NUMERIC(12,2) NOT NULL DEFAULT 0,
    chance_loss_amt NUMERIC(14,2) NOT NULL DEFAULT 0,
    chance_loss_reduction_pct NUMERIC(7,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_id, sku_id)
);

CREATE INDEX IF NOT EXISTS idx_production_prediction_snapshots_lookup
    ON production_prediction_snapshots(store_id, business_date, status, is_active, target_hour DESC);

CREATE INDEX IF NOT EXISTS idx_production_prediction_snapshot_items_snapshot
    ON production_prediction_snapshot_items(snapshot_id, status, predicted_stock_1h);
