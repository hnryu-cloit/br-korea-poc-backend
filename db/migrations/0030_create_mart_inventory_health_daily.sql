-- 매장×품목×일별 재고 건전성 사전 분류 마트
-- 원천: core_stock_rate, core_stockout_time, raw_product_shelf_life

CREATE TABLE IF NOT EXISTS mart_inventory_health_daily (
    store_id              VARCHAR(64)  NOT NULL,
    prc_dt                VARCHAR(8)   NOT NULL,
    item_cd               VARCHAR(128) NOT NULL,
    item_nm               VARCHAR(255) NOT NULL,
    ord_avg               NUMERIC(18, 4) NOT NULL DEFAULT 0,
    sal_avg               NUMERIC(18, 4) NOT NULL DEFAULT 0,
    stk_avg               NUMERIC(18, 4) NOT NULL DEFAULT 0,
    stk_rt                NUMERIC(8, 4)  NOT NULL DEFAULT 0,
    is_stockout           BOOLEAN        NOT NULL DEFAULT FALSE,
    stockout_hour         SMALLINT,
    shelf_life_days       INTEGER        NOT NULL DEFAULT 1,
    expiry_risk_level     VARCHAR(16)    NOT NULL DEFAULT '낮음',
    inventory_status      VARCHAR(16)    NOT NULL DEFAULT '적정', -- 과잉/적정/부족/품절
    generated_at          TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, prc_dt, item_cd, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_inventory_health_daily_store_dt
    ON mart_inventory_health_daily (store_id, prc_dt DESC);
CREATE INDEX IF NOT EXISTS idx_mart_inventory_health_daily_status
    ON mart_inventory_health_daily (inventory_status);
CREATE INDEX IF NOT EXISTS idx_mart_inventory_health_daily_stockout
    ON mart_inventory_health_daily (is_stockout)
    WHERE is_stockout = TRUE;
