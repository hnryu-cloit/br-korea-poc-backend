-- resource/05. 재고 및 품절 신규 적재 테이블

CREATE TABLE IF NOT EXISTS raw_stock_rate (
    STOR_CD          TEXT,
    MASKED_STOR_CD   TEXT,
    MASKED_STOR_NM   TEXT,
    PRC_DT           TEXT,   -- 일자 YYYYMMDD
    ITEM_CD          TEXT,
    ITEM_NM          TEXT,
    ORD_AVG          TEXT,   -- 판매가능수량
    SAL_AVG          TEXT,   -- 판매량
    STK_AVG          TEXT,   -- 재고량 (음수 가능: 품절 초과 판매)
    STK_RT           TEXT,   -- 재고율 (음수 가능)
    source_file      TEXT NOT NULL,
    source_sheet     VARCHAR(255),
    loaded_at        TIMESTAMPTZ NOT NULL
);

-- 품절시간 (CK / JBOD / 기타 3개 파일 통합 적재)
CREATE TABLE IF NOT EXISTS raw_stockout_time (
    STOR_CD          TEXT,
    MASKED_STOR_CD   TEXT,
    MASKED_STOR_NM   TEXT,
    PRC_DT           TEXT,   -- 일자 YYYYMMDD
    ITEM_CD          TEXT,
    ITEM_NM          TEXT,
    STOR_CNT         TEXT,   -- 해당 상품 취급 점포수
    RANKING_MAIN     TEXT,   -- 전체 랭킹
    O_RANKING1       TEXT,   -- 1위 기준 랭킹
    O_RANKING3       TEXT,   -- 3위 기준 랭킹
    ORD_AVG          TEXT,   -- 판매가능수량
    SAL_AVG          TEXT,   -- 판매량
    STK_AVG          TEXT,   -- 재고량 (음수 가능)
    STK_RT           TEXT,   -- 재고율 (음수 가능)
    SOLD_OUT_TM      TEXT,   -- 품절 시각 (숫자 또는 'N시' 형식 혼재)
    source_file      TEXT NOT NULL,
    source_sheet     VARCHAR(255),
    loaded_at        TIMESTAMPTZ NOT NULL
);