CREATE TABLE IF NOT EXISTS core_inferred_stockout_event (
    id BIGSERIAL PRIMARY KEY,
    masked_stor_cd TEXT NOT NULL,
    sale_dt TEXT NOT NULL,
    item_cd TEXT NOT NULL,
    item_nm TEXT NOT NULL,
    is_stockout BOOLEAN NOT NULL DEFAULT TRUE,
    stockout_hour INT NOT NULL,
    rule_type TEXT NOT NULL,
    source_table TEXT NOT NULL,
    open_hour INT,
    close_hour INT,
    zero_sales_window INT,
    evidence_start_hour INT,
    evidence_end_hour INT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_core_inferred_stockout_event_scope
    ON core_inferred_stockout_event(masked_stor_cd, sale_dt, item_cd, rule_type);

CREATE INDEX IF NOT EXISTS ix_core_inferred_stockout_event_store_date
    ON core_inferred_stockout_event(masked_stor_cd, sale_dt);

CREATE INDEX IF NOT EXISTS ix_core_inferred_stockout_event_date_hour
    ON core_inferred_stockout_event(sale_dt, stockout_hour);
