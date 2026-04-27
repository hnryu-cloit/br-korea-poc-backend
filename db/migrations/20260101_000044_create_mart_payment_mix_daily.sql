-- 매장×일별 결제 믹스 마트
-- 원천: raw_daily_store_pay_way (pay_way_cd / pay_way_cd_nm 단위 합산)

CREATE TABLE IF NOT EXISTS mart_payment_mix_daily (
    store_id              VARCHAR(64)  NOT NULL,
    sale_dt               VARCHAR(8)   NOT NULL,
    pay_way_cd            VARCHAR(16)  NOT NULL,
    pay_way_cd_nm         VARCHAR(64),
    pay_amt               NUMERIC(18, 2) NOT NULL DEFAULT 0,
    pay_count             INTEGER        NOT NULL DEFAULT 0,
    rtn_pay_amt           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    share_ratio           NUMERIC(8, 4)  NOT NULL DEFAULT 0,
    is_delivery_channel   BOOLEAN        NOT NULL DEFAULT FALSE, -- pay_way_cd = '18'
    is_discount_channel   BOOLEAN        NOT NULL DEFAULT FALSE, -- 03/19
    generated_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, sale_dt, pay_way_cd)
);

CREATE INDEX IF NOT EXISTS idx_mart_payment_mix_daily_store_dt
    ON mart_payment_mix_daily (store_id, sale_dt DESC);
CREATE INDEX IF NOT EXISTS idx_mart_payment_mix_daily_dt
    ON mart_payment_mix_daily (sale_dt);
