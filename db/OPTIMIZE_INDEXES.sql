-- [BR-Korea PoC] 데이터베이스 완전체 최적화 스크립트 (통합 버전)
-- 새로운 인스턴스 배포 시 자동 초기화를 위해 설계됨

-- 1. 대량 원본 데이터 테이블 (Full Table Scan 방지)
-- 1.1 일일 판매 아이템 (약 70만 건)
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_main ON raw_daily_store_item (masked_stor_cd, sale_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_dt ON raw_daily_store_item (sale_dt);

-- 1.2 시간대별 매출 (약 280만 건 - 가장 중요한 병목 지점)
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_main ON raw_daily_store_item_tmzon (masked_stor_cd, sale_dt, tmzon_div);
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_item ON raw_daily_store_item_tmzon (item_cd);
-- 함수 기반 색인 (REPLACE/CAST 패턴 대응)
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_func_date ON raw_daily_store_item_tmzon (REPLACE(CAST(sale_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_func_store_date ON raw_daily_store_item_tmzon (masked_stor_cd, REPLACE(CAST(sale_dt AS TEXT), '-', ''));

-- 1.3 생산 및 발주 추출 데이터 (약 50만 건)
CREATE INDEX IF NOT EXISTS idx_raw_production_main ON raw_production_extract (masked_stor_cd, prod_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_production_func_date ON raw_production_extract (REPLACE(CAST(prod_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_order_main ON raw_order_extract (masked_stor_cd, dlv_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_order_func_date ON raw_order_extract (REPLACE(CAST(dlv_dt AS TEXT), '-', ''));

-- 1.4 재고 및 FIFO 관리
CREATE INDEX IF NOT EXISTS idx_raw_inventory_main ON raw_inventory_extract (masked_stor_cd, stock_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_inventory_fifo_active ON inventory_fifo_lots (masked_stor_cd, item_nm, status, expiry_date) WHERE status = 'active';

-- 2. 마스터 및 클러스터 데이터 (조인 성능 최적화)
CREATE INDEX IF NOT EXISTS idx_raw_store_master_cd ON raw_store_master (masked_stor_cd);
CREATE INDEX IF NOT EXISTS idx_raw_store_embedding_hnsw ON raw_store_master USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_store_clusters_lookup ON store_clusters (masked_stor_cd, cluster_id);

-- 3. 공통 마트 테이블 (대시보드 KPI 조회용)
CREATE INDEX IF NOT EXISTS idx_mart_store_kpi_main ON mart_store_daily_kpi (masked_stor_cd, sale_dt);
CREATE INDEX IF NOT EXISTS idx_mart_product_price_dt ON mart_product_price_daily (item_cd, sale_dt);

-- 4. 운영 및 감사 로그 (사용자 액션 트래킹)
CREATE INDEX IF NOT EXISTS idx_ordering_selections_main ON ordering_selections (store_id, selected_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_lookup ON audit_logs (domain, event_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_user_bookmarks_store ON user_bookmarks (store_id, type);

-- 5. 캠페인 및 할인 정보 (날짜 범위 검색 대응)
CREATE INDEX IF NOT EXISTS idx_raw_campaign_period ON raw_campaign_master (REPLACE(CAST(start_dt AS TEXT), '-', ''), REPLACE(CAST(fnsh_dt AS TEXT), '-', ''));
