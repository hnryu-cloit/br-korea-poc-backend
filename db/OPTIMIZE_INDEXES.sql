-- [BR-Korea PoC] 데이터베이스 완전체 최적화 스크립트 (통합 버전)
-- 새로운 인스턴스 배포 시 자동 초기화를 위해 설계됨

-- 1. 대량 원본 데이터 테이블 (Full Table Scan 방지)
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_main ON raw_daily_store_item (masked_stor_cd, sale_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_daily_item_name_sales ON raw_daily_store_item (masked_stor_cd, item_nm, sale_dt DESC, sale_amt);

-- 1.2 시간대별 매출 (280만 건 - 가장 중요한 병목)
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_main ON raw_daily_store_item_tmzon (masked_stor_cd, sale_dt, tmzon_div);
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_item ON raw_daily_store_item_tmzon (item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_item_tmzon_func_store_date ON raw_daily_store_item_tmzon (masked_stor_cd, REPLACE(CAST(sale_dt AS TEXT), '-', ''));

-- 1.3 결제 및 채널 분석
CREATE INDEX IF NOT EXISTS idx_raw_pay_way_main ON raw_daily_store_pay_way (masked_stor_cd, sale_dt, pay_dtl_cd);
CREATE INDEX IF NOT EXISTS idx_raw_pay_cd_lookup ON raw_pay_cd (pay_dc_cd, pay_dc_nm);

-- 1.4 생산 및 발주 데이터
CREATE INDEX IF NOT EXISTS idx_raw_production_main ON raw_production_extract (masked_stor_cd, prod_dt, item_cd);
CREATE INDEX IF NOT EXISTS idx_raw_order_main ON raw_order_extract (masked_stor_cd, dlv_dt, item_cd);

-- 1.5 재고 및 FIFO
CREATE INDEX IF NOT EXISTS idx_raw_inventory_main ON raw_inventory_extract (masked_stor_cd, stock_dt, item_cd);

-- 2. 캠페인 최적화 색인 (Prompts API 지연 해결의 핵심)
CREATE INDEX IF NOT EXISTS idx_raw_campaign_master_active ON raw_campaign_master (use_yn, start_dt, fnsh_dt);
CREATE INDEX IF NOT EXISTS idx_raw_campaign_item_cd ON raw_campaign_item (cpi_cd);
CREATE INDEX IF NOT EXISTS idx_raw_campaign_group_cd ON raw_campaign_item_group (cpi_cd);

-- 3. 마스터 및 클러스터
CREATE INDEX IF NOT EXISTS idx_raw_store_master_cd ON raw_store_master (masked_stor_cd);
CREATE INDEX IF NOT EXISTS idx_raw_store_embedding_hnsw ON raw_store_master USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_store_clusters_lookup ON store_clusters (masked_stor_cd, cluster_id);

-- 4. 운영 로그 및 대시보드
CREATE INDEX IF NOT EXISTS idx_ordering_selections_main ON ordering_selections (store_id, selected_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_lookup ON audit_logs (domain, event_type, timestamp DESC);
