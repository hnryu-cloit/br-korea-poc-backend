-- 인덱스 최적화: raw_daily_store_item / raw_daily_store_item_tmzon
--
-- 배경: 두 테이블이 매출 요약/대시보드 쿼리에서 자주 조회되지만 인덱스가 없어
-- full scan 발생. EXPLAIN ANALYZE 검증 결과 다음 개선:
--   Q1 (점포+기간):    201ms → 2ms     (~100x)
--   Q2 (일자별 집계):   87ms → 68ms    (~1.3x, Bitmap Index Scan)
--   Q3 (시간대별 매출): 383ms → 15ms   (~25x, expression index)
--
-- 주의: tmzon_div는 TEXT인데 코드는 CAST(tmzon_div AS INTEGER)로 비교.
-- 일반 인덱스는 CAST 때문에 부분 적용만 되어 expression index를 함께 둠.

-- raw_daily_store_item: 점포별 기간 조회용
CREATE INDEX IF NOT EXISTS idx_raw_daily_store_item_store_date
    ON raw_daily_store_item (masked_stor_cd, sale_dt);

-- raw_daily_store_item: 일자별 전사 집계용
CREATE INDEX IF NOT EXISTS idx_raw_daily_store_item_date_store
    ON raw_daily_store_item (sale_dt, masked_stor_cd);

-- raw_daily_store_item_tmzon: 일반 (sale_dt, tmzon_div, masked_stor_cd)
CREATE INDEX IF NOT EXISTS idx_raw_daily_store_item_tmzon_hour
    ON raw_daily_store_item_tmzon (sale_dt, tmzon_div, masked_stor_cd);

-- raw_daily_store_item_tmzon: CAST(tmzon_div AS INTEGER) 쿼리용 expression index
CREATE INDEX IF NOT EXISTS idx_raw_daily_store_item_tmzon_hour_expr
    ON raw_daily_store_item_tmzon (sale_dt, (CAST(tmzon_div AS INTEGER)));
