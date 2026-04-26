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
    expiry_risk_level VARCHAR(16) NOT NULL DEFAULT '낮음',
    status VARCHAR(16) NOT NULL DEFAULT '적정',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_date, item_cd)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_production_inventory_status_lookup
    ON mart_poc_010_production_inventory_status(business_date DESC, stock_rate ASC, item_nm ASC);
