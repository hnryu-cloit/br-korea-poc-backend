-- 품목 카테고리 사전 분류 마트
-- 원천: core_daily_item_sales 의 distinct item_nm
-- 분류 기준: 키워드 매칭(스크립트에서 산출)

CREATE TABLE IF NOT EXISTS mart_item_category_master (
    item_cd            VARCHAR(128) NOT NULL,
    item_nm            VARCHAR(255) NOT NULL,
    category           VARCHAR(64)  NOT NULL DEFAULT '기타',
    is_coffee          BOOLEAN      NOT NULL DEFAULT FALSE,
    is_drink           BOOLEAN      NOT NULL DEFAULT FALSE,
    is_food            BOOLEAN      NOT NULL DEFAULT FALSE,
    is_seasonal        BOOLEAN      NOT NULL DEFAULT FALSE,
    season_tag         VARCHAR(64),
    parent_item_nm     VARCHAR(255),
    keyword_match      VARCHAR(255),
    sample_qty         NUMERIC(18, 2) NOT NULL DEFAULT 0,
    updated_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_cd, item_nm)
);

CREATE INDEX IF NOT EXISTS idx_mart_item_category_master_item_nm
    ON mart_item_category_master (item_nm);
CREATE INDEX IF NOT EXISTS idx_mart_item_category_master_category
    ON mart_item_category_master (category);
CREATE INDEX IF NOT EXISTS idx_mart_item_category_master_is_coffee
    ON mart_item_category_master (is_coffee)
    WHERE is_coffee = TRUE;
