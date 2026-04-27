CREATE TABLE IF NOT EXISTS raw_store_production_item (
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    item_cd TEXT,
    item_nm TEXT,
    source_file TEXT NOT NULL,
    source_sheet VARCHAR(255),
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_store_production_item_store_item
    ON raw_store_production_item(masked_stor_cd, item_cd);
