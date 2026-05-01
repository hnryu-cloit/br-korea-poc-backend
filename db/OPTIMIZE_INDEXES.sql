-- [최적화] 함수 기반 인덱스 (백엔드 코드의 REPLACE/CAST 패턴 대응)

-- 1. 일일 시간대별 매출 테이블 (가장 큰 병목)
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_func_date ON raw_daily_store_item_tmzon (REPLACE(CAST(sale_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_func_store_date ON raw_daily_store_item_tmzon (masked_stor_cd, REPLACE(CAST(sale_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_item_cd ON raw_daily_store_item_tmzon (item_cd);

-- 2. 생산 추출 테이블
CREATE INDEX IF NOT EXISTS idx_raw_prod_func_date ON raw_production_extract (REPLACE(CAST(prod_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_prod_func_store_date ON raw_production_extract (masked_stor_cd, REPLACE(CAST(prod_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_prod_item_cd ON raw_production_extract (item_cd);

-- 3. 발주 추출 테이블
CREATE INDEX IF NOT EXISTS idx_raw_order_func_date ON raw_order_extract (REPLACE(CAST(dlv_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_order_func_store_date ON raw_order_extract (masked_stor_cd, REPLACE(CAST(dlv_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_order_item_cd ON raw_order_extract (item_cd);

-- 4. 기존 일반 인덱스 유지 및 보강
CREATE INDEX IF NOT EXISTS idx_raw_store_master_cd ON raw_store_master (masked_stor_cd);
CREATE INDEX IF NOT EXISTS idx_raw_store_embedding_hnsw ON raw_store_master USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_audit_logs_domain_event ON audit_logs (domain, event_type, timestamp);
