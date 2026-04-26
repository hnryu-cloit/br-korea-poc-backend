-- 매장×일별 핵심 KPI 통합 마트
-- 원천: raw_daily_store_channel, raw_daily_store_pay_way, core_daily_item_sales,
--       mart_item_category_master, raw_weather_daily, raw_store_master

CREATE TABLE IF NOT EXISTS mart_store_daily_kpi (
    store_id                 VARCHAR(64) NOT NULL,
    sale_dt                  VARCHAR(8)  NOT NULL,
    -- 매출/주문 기본
    total_sales              NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_orders             NUMERIC(18, 2) NOT NULL DEFAULT 0,
    -- 채널 분리
    delivery_sales           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    takeout_orders           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    in_store_pay_count       INTEGER        NOT NULL DEFAULT 0,
    -- 시간대 평균 단가 (런치 ~15시 / 스윙 15~17시 / 디너 17시~)
    lunch_avg_ticket         NUMERIC(18, 2) NOT NULL DEFAULT 0,
    swing_avg_ticket         NUMERIC(18, 2) NOT NULL DEFAULT 0,
    dinner_avg_ticket        NUMERIC(18, 2) NOT NULL DEFAULT 0,
    -- 카테고리별 매출 (mart_item_category_master 기반)
    coffee_sales             NUMERIC(18, 2) NOT NULL DEFAULT 0,
    drink_sales              NUMERIC(18, 2) NOT NULL DEFAULT 0,
    food_sales               NUMERIC(18, 2) NOT NULL DEFAULT 0,
    coffee_attach_ratio      NUMERIC(8, 4)  NOT NULL DEFAULT 0,
    -- 일별 1위 메뉴
    top_item_nm              VARCHAR(255),
    top_item_sales           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    -- 날씨 (raw_weather_daily 조인, 매장 sido 기준)
    avg_temp_c               NUMERIC(8, 2),
    precipitation_mm         NUMERIC(8, 2),
    -- 메타
    item_count               INTEGER        NOT NULL DEFAULT 0,
    generated_at             TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, sale_dt)
);

CREATE INDEX IF NOT EXISTS idx_mart_store_daily_kpi_sale_dt
    ON mart_store_daily_kpi (sale_dt);
CREATE INDEX IF NOT EXISTS idx_mart_store_daily_kpi_store_dt
    ON mart_store_daily_kpi (store_id, sale_dt DESC);
