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
    expiry_risk_level VARCHAR(16) NOT NULL DEFAULT '낮음',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (target_month, item_cd, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_production_waste_monthly_target_month
    ON mart_poc_010_production_waste_monthly(target_month DESC, total_waste_qty DESC);
