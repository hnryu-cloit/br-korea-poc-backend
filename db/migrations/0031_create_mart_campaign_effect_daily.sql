-- 캠페인×일별 효과 마트
-- 원천: raw_campaign_master (cpi_cd, start_dt~fnsh_dt), raw_campaign_item (item_cd 매핑),
--       core_daily_item_sales (참여 매장 매출/할인)

CREATE TABLE IF NOT EXISTS mart_campaign_effect_daily (
    cpi_cd                       VARCHAR(64) NOT NULL,
    cpi_nm                       VARCHAR(255),
    sale_dt                      VARCHAR(8)  NOT NULL,
    applicable_item_count        INTEGER     NOT NULL DEFAULT 0,
    total_sales_during           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_dc_amt                 NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_qty                    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    participating_store_count    INTEGER     NOT NULL DEFAULT 0,
    baseline_sales_avg           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    sales_lift_ratio             NUMERIC(8, 4)  NOT NULL DEFAULT 0,
    cpi_dc_type_nm               VARCHAR(64),
    discount_value               NUMERIC(18, 2),
    generated_at                 TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cpi_cd, sale_dt)
);

CREATE INDEX IF NOT EXISTS idx_mart_campaign_effect_daily_cpi
    ON mart_campaign_effect_daily (cpi_cd);
CREATE INDEX IF NOT EXISTS idx_mart_campaign_effect_daily_dt
    ON mart_campaign_effect_daily (sale_dt);
