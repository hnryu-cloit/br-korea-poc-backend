-- [BR-Korea PoC] 데이터베이스 완전체 최적화 스크립트 (통합 및 Prompts 보강 버전)

-- 1. 대량 원본 데이터 테이블 (Full Table Scan 방지)
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_main ON raw_daily_store_item (masked_stor_cd, sale_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_dt ON raw_daily_store_item (sale_dt);
-- [보강] 상품별 매출 순위 및 이상징후 윈도우 함수 최적화
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_name_sales ON raw_daily_store_item (masked_stor_cd, item_nm, sale_dt DESC, sale_amt);

-- 1.2 시간대별 매출 (280만 건 - 가장 중요한 병목)
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_main ON raw_daily_store_item_tmzon (masked_stor_cd, sale_dt, tmzon_div);
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_item ON raw_daily_store_item_tmzon (item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_func_store_date ON raw_daily_store_item_tmzon (masked_stor_cd, REPLACE(CAST(sale_dt AS TEXT), '-', ''));

-- 1.3 결제 및 채널 분석 최적화
CREATE INDEX IF NOT EXISTS idx_raw_pay_way_main ON raw_daily_store_pay_way (masked_stor_cd, sale_dt, pay_dtl_cd);
CREATE INDEX IF NOT EXISTS idx_raw_pay_cd_lookup ON raw_pay_cd (pay_dc_cd, pay_dc_nm);

-- 1.4 생산 및 발주 추출 데이터
CREATE INDEX IF NOT EXISTS idx_raw_production_main ON raw_production_extract (masked_stor_cd, prod_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_production_func_date ON raw_production_extract (REPLACE(CAST(prod_dt AS TEXT), '-', ''));
CREATE INDEX IF NOT EXISTS idx_raw_order_main ON raw_order_extract (masked_stor_cd, dlv_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_order_func_date ON raw_order_extract (REPLACE(CAST(dlv_dt AS TEXT), '-', ''));

-- 1.5 재고 및 FIFO 관리
CREATE INDEX IF NOT EXISTS idx_raw_inventory_main ON raw_inventory_extract (masked_stor_cd, stock_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_inventory_fifo_active ON inventory_fifo_lots (masked_stor_cd, item_nm, status, expiry_date) WHERE status = 'active';

-- 2. 마스터 및 클러스터 데이터 (조인 최적화)
CREATE INDEX IF NOT EXISTS idx_raw_store_master_cd ON raw_store_master (masked_stor_cd);
CREATE INDEX IF NOT EXISTS idx_raw_store_embedding_hnsw ON raw_store_master USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_store_clusters_lookup ON store_clusters (masked_stor_cd, cluster_id);

-- 3. 공통 마트 테이블 (대시보드 KPI)
CREATE INDEX IF NOT EXISTS idx_mart_store_kpi_main ON mart_store_daily_kpi (masked_stor_cd, sale_dt);
CREATE INDEX IF NOT EXISTS idx_mart_product_price_dt ON mart_product_price_daily (item_cd, sale_dt);

-- 4. 운영 및 감사 로그
CREATE INDEX IF NOT EXISTS idx_ordering_selections_main ON ordering_selections (store_id, selected_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_lookup ON audit_logs (domain, event_type, timestamp DESC);

-- 5. 캠페인 및 할인 정보
CREATE INDEX IF NOT EXISTS idx_raw_campaign_period ON raw_campaign_master (REPLACE(CAST(start_dt AS TEXT), '-', ''), REPLACE(CAST(fnsh_dt AS TEXT), '-', ''));
