-- 매장×요일×시간대 평균 매출/주문 (4주 이동) 마트
-- 원천: raw_daily_store_channel (sale_dt, tmzon_div, sale_amt, ord_cnt)

CREATE TABLE IF NOT EXISTS mart_hourly_sales_pattern (
    store_id              VARCHAR(64) NOT NULL,
    dow                   SMALLINT    NOT NULL, -- 0=월 ~ 6=일
    hour                  SMALLINT    NOT NULL, -- 0~23
    avg_sale_amt          NUMERIC(18, 2) NOT NULL DEFAULT 0,
    avg_ord_cnt           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    sample_day_count      INTEGER        NOT NULL DEFAULT 0,
    peak_rank             SMALLINT       NOT NULL DEFAULT 0, -- 매장×요일 내 시간대 매출 순위 (1=피크)
    window_start_dt       VARCHAR(8),
    window_end_dt         VARCHAR(8),
    generated_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, dow, hour)
);

CREATE INDEX IF NOT EXISTS idx_mart_hourly_sales_pattern_store
    ON mart_hourly_sales_pattern (store_id);
CREATE INDEX IF NOT EXISTS idx_mart_hourly_sales_pattern_peak
    ON mart_hourly_sales_pattern (store_id, peak_rank);
