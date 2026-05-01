-- 1. 일일 판매 아이템 테이블 인덱스 (기존 인덱스 보강)
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_store_date_item ON raw_daily_store_item (masked_stor_cd, sale_dt, item_cd);

-- 2. 생산 추출 데이터 인덱스
CREATE INDEX IF NOT EXISTS idx_raw_production_main ON raw_production_extract (masked_stor_cd, prod_dt, item_cd);

-- 3. 발주 추출 데이터 인덱스
CREATE INDEX IF NOT EXISTS idx_raw_order_main ON raw_order_extract (masked_stor_cd, dlv_dt, item_cd);

-- 4. 점포 마스터 인덱스 (조인 성능 향상)
CREATE INDEX IF NOT EXISTS idx_raw_store_master_cd ON raw_store_master (masked_stor_cd);

-- 5. 점포 마스터 벡터 검색 인덱스 (HNSW)
CREATE INDEX IF NOT EXISTS idx_raw_store_embedding_hnsw ON raw_store_master USING hnsw (embedding vector_cosine_ops);

-- 6. 발주 선택 내역 인덱스
CREATE INDEX IF NOT EXISTS idx_ordering_selections_store_at ON ordering_selections (store_id, selected_at);

-- 7. 대시보드 알림 및 감사 로그
CREATE INDEX IF NOT EXISTS idx_audit_logs_domain_event ON audit_logs (domain, event_type, timestamp);
