-- 전 지점 통합 제품 판매가 마트 (평상시 가격 + 프로모션 시점 가격)
-- 원천: core_daily_item_sales, raw_campaign_master, raw_campaign_item

CREATE TABLE IF NOT EXISTS mart_product_price_daily (
    item_cd                 VARCHAR(128) NOT NULL,
    item_nm                 VARCHAR(255) NOT NULL,
    price_dt                VARCHAR(8)   NOT NULL,
    list_price              NUMERIC(18, 2) NOT NULL DEFAULT 0,
    net_price               NUMERIC(18, 2) NOT NULL DEFAULT 0,
    discount_amount         NUMERIC(18, 2) NOT NULL DEFAULT 0,
    discount_rate           NUMERIC(8, 4)  NOT NULL DEFAULT 0,
    sample_store_count      INTEGER        NOT NULL DEFAULT 0,
    sample_qty              NUMERIC(18, 2) NOT NULL DEFAULT 0,
    is_promotion            BOOLEAN        NOT NULL DEFAULT FALSE,
    matched_campaign_codes  TEXT[]         NOT NULL DEFAULT ARRAY[]::TEXT[],
    generated_at            TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_cd, item_nm, price_dt)
);

CREATE INDEX IF NOT EXISTS idx_mart_product_price_daily_item_nm
    ON mart_product_price_daily (item_nm);
CREATE INDEX IF NOT EXISTS idx_mart_product_price_daily_price_dt
    ON mart_product_price_daily (price_dt);
CREATE INDEX IF NOT EXISTS idx_mart_product_price_daily_promo
    ON mart_product_price_daily (is_promotion)
    WHERE is_promotion = TRUE;


CREATE TABLE IF NOT EXISTS mart_product_price_master (
    item_cd                 VARCHAR(128) NOT NULL,
    item_nm                 VARCHAR(255) NOT NULL,
    regular_list_price      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    regular_net_price       NUMERIC(18, 2) NOT NULL DEFAULT 0,
    latest_list_price       NUMERIC(18, 2) NOT NULL DEFAULT 0,
    latest_net_price        NUMERIC(18, 2) NOT NULL DEFAULT 0,
    price_change_count      INTEGER        NOT NULL DEFAULT 0,
    last_price_change_dt    VARCHAR(8),
    active_promotion_count  INTEGER        NOT NULL DEFAULT 0,
    sample_day_count        INTEGER        NOT NULL DEFAULT 0,
    updated_at              TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_cd, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_product_price_master_item_nm
    ON mart_product_price_master (item_nm);
