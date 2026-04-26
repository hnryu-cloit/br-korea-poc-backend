# br-korea-poc-backend

BR Korea 매장 운영 지원 POC의 백엔드 API 서버입니다. FastAPI 기반의 REST API, PostgreSQL 데이터 적재 파이프라인, 감사 로그·운영 이력 관리 기능을 포함합니다. 현재 인터페이스 기준은 `br-korea-poc-front`입니다.

## 최근 업데이트 (2026-04-26)

- 사전 계산 마트 6종을 추가로 도입했습니다 (분석/대시보드 화면의 반복 SQL 부하 해소). 일괄 재계산은 `python scripts/load_all_marts.py`.
  - 첫 활용처: `analytics_repository._fetch_in_store_pay_counts()`가 `mart_store_daily_kpi.in_store_pay_count` 합산을 1순위로 사용하고 raw 카운트를 폴백으로 둡니다.
  - `mart_item_category_master` (0027): 660품목 카테고리 사전 분류 (`is_coffee`, `is_seasonal`, `parent_item_nm` 등). 기존 `LIKE '%커피%'` 분기를 대체합니다.
  - `mart_store_daily_kpi` (0028): 매장×일별 1행 KPI 통합 — total_sales, delivery_sales, takeout_orders, in_store_pay_count, 시간대 평균 단가 3종, 카테고리별 매출, top_item, 날씨.
  - `mart_hourly_sales_pattern` (0029): 매장×요일×시간대 28일 평균 + peak_rank. SalesTrendChart hour/dow_points 가속.
  - `mart_inventory_health_daily` (0030): 매장×품목×일별(90일) 재고 건전성 사전 분류(과잉/적정/부족/품절).
  - `mart_campaign_effect_daily` (0031): 캠페인×일별 매출/할인/lift 비율. baseline은 시작 직전 14일 평균.
  - `mart_payment_mix_daily` (0032): 매장×일별×결제수단 영수증 단위 합산 + share_ratio.

- production inventory-status 리팩토링을 적용했습니다.
  - `ProductionService.get_inventory_status()` 캐시 키에 `business_date`를 포함해 기준일별 캐시 오염 가능성을 제거했습니다.
  - `ProductionRepository.get_inventory_status()`의 non-mart 경로에서 반복되던 `latest/filtered` CTE를 공통 SQL 템플릿으로 통합해 요약/카운트/목록 조회 정합성을 높였습니다.

- 전 지점 통합 제품 판매가 마트(`mart_product_price_daily`, `mart_product_price_master`) 2종을 추가했습니다.
  - 마이그레이션: `db/migrations/0026_create_mart_product_price.sql`
  - 적재 스크립트: `scripts/load_mart_product_price.py` — `core_daily_item_sales` 전 지점 일별 집계 + `raw_campaign_master`/`raw_campaign_item` 매칭으로 평상시 정가(mode), 평상시 실단가, 최근 30일 평균, 가격 변동 횟수, 진행중 프로모션 수를 사전 계산합니다.
  - `mart_product_price_daily`는 일별 시계열(`is_promotion`, `matched_campaign_codes` 포함)이며, `mart_product_price_master`는 품목별 1행 룩업입니다.
  - 폐기 손실 API(`get_production_waste_rows`)의 `unit_price` CTE를 마트 우선 + 전 지점 평균 폴백으로 교체해 기간 데이터 부재 시에도 판매가가 0으로 빠지지 않도록 했습니다.

## 최근 업데이트 (2026-04-25)

- `GET /api/production/fifo-lots` 집계 기준을 기준일 당일(`lot_date = date`)로 변경했습니다.
  - 기존 기준일 이전 누적 집계(`lot_date <= date`)를 당일 집계로 전환해, 화면의 "기준일시 기준" 의미와 API 결과를 일치시켰습니다.
  - endpoint `date` 파라미터 설명도 "해당 일자 lot_date만 집계"로 동기화했습니다.

- `GET /api/analytics/metrics` 응답 KPI를 매출 현황 운영 지표 7종으로 재구성했습니다 (앱 주문 비중·할인 결제 비중·선택 기간 총 매출(items)·평균 객단가·기존 배달 건수 제거 → 투고 건수·배달 매출액·매장 결제 건수·커피 동반 구매율·런치/스윙/디너 시간대 평균 단가).
  - `app/repositories/analytics_repository.py`의 `_get_channel_metrics()`가 `raw_daily_store_channel.tmzon_div`를 활용해 시간대(런치 ~15시 / 스윙 15~17시 / 디너 17시~) 평균 단가와 투고/배달 매출 분리를 계산합니다.
  - 매장 결제 건수는 `raw_daily_store_pay_way`에서 `pay_way_cd != '18'`(배달/해피오더 제외) 영수증을 카운트해 산출합니다 — POC 데이터에 오프라인 채널 컬럼이 없어 추정치인 "홀 방문 고객" 대신 실측 결제 건수로 라벨링합니다.
  - `selected_period_total_sales` 필드는 그대로 유지되어 매출 현황 화면 상단 큰 카드에서 사용됩니다.
- 런타임 오류를 수정했습니다.
  - `app/repositories/ordering_repository.py`의 `logging` 미임포트로 인한 `NameError` 가능성을 제거했습니다.
  - `app/repositories/sales_repository.py`의 미정의 변수(`max_dt_row`) 참조를 제거하고 기존 `date_bounds_row` 기준 흐름으로 정리했습니다.
  - 데이터 쿼리(SQL) 조건/로직은 변경하지 않았습니다.
- `POST /api/sales/query` 요청 스키마를 실행 컨텍스트 기반으로 확장했습니다.
  - `SalesQueryRequest`에 `business_time`, `page_context`, `card_context_key`, `store_name`, `user_role`, `conversation_history` 필드를 추가했습니다.
  - `ChatHistoryEntry(role, text)` 모델을 신설해 직전 6턴 대화 이력을 표준화했습니다.
  - `app/services/ai_client.py`의 `query_sales()`가 신규 컨텍스트 필드를 받아 AI 서버 `/sales/query`로 그대로 포워딩합니다.
  - `app/services/sales_service.py`에서 payload의 `conversation_history`를 `model_dump()` 후 AI 클라이언트에 전달합니다.
  - 기존 `prompt`, `store_id`, `domain`, `business_date` 호출 계약은 그대로 유지됩니다.
- analytics 엔드포인트 반복 예외 처리 패턴을 공통 함수로 정리했습니다.
  - `app/api/v1/endpoints/analytics.py`에 기준일시 해석 공통 함수(`_resolve_reference_range_or_422`)와 런타임 500 변환 함수(`_raise_runtime_500`)를 추가했습니다.
  - `/analytics/metrics`, `/analytics/sales-trend`의 기존 오류 코드/메시지 계약은 유지됩니다.
- AI/프론트 리팩토링 세션 연동 사항을 반영했습니다.
  - AI `management` 라우터의 ML 예측 보조 로직이 서비스 계층으로 이동했고, 프론트 매출 차트 컴포넌트 생성 패턴이 정리되었습니다.
  - 본 세션의 backend API/DB 스키마 변경은 없습니다.
- 상권 필터 옵션 조회 API를 추가했습니다.
  - `GET /api/analytics/market-scope-options`에서 `gu_options`, `dong_options_by_gu`를 반환합니다.
  - `raw_seoul_market_sales`, `raw_seoul_market_floating_population`의 `area_name`을 기반으로 동적 구성하며, 데이터 부재 시 서울 25개 구 기본 목록을 반환합니다.
- 소진공 연계 `areaCd` 매핑을 서울 25개 구 전체로 확장했습니다.
  - 기존 5개 구 하드코딩 분기를 제거하고 공통 매핑(`_SEOUL_GU_AREA_CODE`)으로 통합했습니다.
- 프론트 콘솔 `sales` 404/500 원인을 로그로 확인했습니다.
  - `raw_daily_store_channel` 집계 쿼리에서 `SUM(text)` 타입 오류가 발생해 `insights/campaign-effect`가 500으로 실패합니다.
  - `summary` 404는 no-fallback 정책에 따라 데이터 부재 시 반환되는 정상 분기입니다.
- `sales` API 안정화 패치를 적용했습니다.
  - `raw_daily_store_channel` 집계 쿼리(`insights`, `query channel`)를 숫자 안전 캐스팅으로 수정해 `SUM(text)` 500 오류를 방지했습니다.
  - AI 서비스가 비활성/오류여도 `GET /api/sales/insights`, `GET /api/sales/campaign-effect`는 repository 실데이터 요약(기본문구)으로 응답하도록 폴백했습니다.
  - `GET /api/sales/summary`는 데이터가 없을 때 404 대신 0값 기본 구조(weekly/top_products 빈 배열)로 응답하도록 완화했습니다.
- 프론트 `/analytics/market` 사이드바 active 충돌 수정과 상권 인사이트 fallback 렌더링 제거 작업을 연동 기준으로 반영했습니다.
- 이번 세션의 DB 마이그레이션 변경은 없습니다.

## 최근 업데이트 (2026-04-24)

- AI grounded 입력 안정화(행 상한 + 프롬프트 예산) 반영 사항을 동기화했습니다.
  - AI grounded 경로 SQL 실행 결과는 `fetchmany(300)` 상한으로 조회됩니다.
  - Gemini 입력의 `reference_data.rows`는 기본 60행 + JSON 길이 예산(18,000자) 기준으로 제한됩니다.
  - `included_row_count`, `truncated`, `omitted_row_count` 메타정보가 추가되어 절단 여부를 확인할 수 있습니다.
  - 이번 세션의 backend API/스키마/마이그레이션 코드는 변경하지 않았습니다.

- AI 전체 스크립트 경로 점검 확장 결과를 반영했습니다.
  - AI 레포 추가 보강 대상(`pipeline/run.py`, `tests/grounded_consistency_utils.py`, `tests/test_golden_query_resolver.py`, `tests/test_grounded_workflow.py`)이 모노레포 루트 실행에서도 동작하도록 정비되었습니다.
  - 이번 세션의 backend API/스키마/마이그레이션 코드는 변경하지 않았습니다.

- 이월재고(FIFO) API/적재 동작 기준을 문서에 동기화했습니다.
  - `GET /api/production/fifo-lots`: `store_id`, `lot_type(production|delivery)`, `page`, `page_size`를 지원합니다.
  - Lot 적재/소진/만료 확정은 `scripts/load_resource_to_db.py`의 `populate_fifo_lots_for_store()`에서 수행합니다.
  - DDL은 `db/migrations/0022_create_inventory_fifo_lots.sql` 기준입니다.
- 운영 기동 의존성 주의사항을 반영했습니다.
  - `docker-compose.yml`에서 `backend`는 `load`가 `service_completed_successfully` 상태가 되어야 기동합니다.
  - `load`가 실행 중이면 `6002` 포트 API가 아직 열리지 않을 수 있습니다.

- AI 레포 골든쿼리 테스트 자산 Git 추적 제외 정책이 반영되었습니다.
  - `br-korea-poc-ai/.gitignore`에 `tests/*golden_query*` 패턴이 추가되었고 기존 추적 파일은 인덱스에서 제거되었습니다.
  - 이번 세션의 backend API/스키마/마이그레이션 변경은 없습니다.

- AI 스크립트 실행 경로 안정화 패치(benchmark/qa/pipeline import 경로 보강)가 반영되었습니다.
  - 이번 세션의 backend API/스키마/마이그레이션 코드는 변경하지 않았습니다.

- AI 레포에서 골든쿼리 홀드아웃 100건 재검증이 수행되었습니다.
  - 이번 세션의 backend API/스키마/마이그레이션 코드는 변경하지 않았습니다.

- 골든쿼리 매칭 벤치마크 세션을 반영했습니다.
  - 이번 세션의 코드 변경은 AI 레포 중심이며, backend API/스키마 변경은 없습니다.

- 프론트 `/settings/connectors` DB 기준 보강과 연계해 현재 시스템 계층 기준을 재확인했습니다.
  - 기준 객체 수: `Raw 23`, `Core 4`, `운영 3` (총 30)
  - 이번 세션의 backend API/스키마/마이그레이션 변경은 없습니다.

- 플로팅챗 골든쿼리 추적 메타데이터를 확장했습니다.
  - AI 응답의 `matched_query_id` `match_score` `overlap_candidates`를 `app/services/ai_client.py`에서 수신해 `agent_trace`로 전달합니다.
  - `app/schemas/sales.py` 계약에 `overlap_candidates`를 추가해 어떤 골든쿼리와 유사한지 백엔드 응답에서 확인할 수 있습니다.
- 골든쿼리 자산을 Gemini 실호출로 보강했습니다.
  - `docs/golden-queries.csv`에 `의도ID` `동의어` 컬럼을 반영했고 초기 보강 백업(`docs/golden-queries.csv.bak`)을 생성했습니다.

- 주문 추천 수량 산식 고도화를 반영했습니다. (프론트 UI 변경 없음)
  - `raw_daily_store_item` 최근 7일 판매추세(`trend_factor`), `raw_inventory_extract` 현재 재고, `raw_product_shelf_life` 유통기한 리스크를 조합해 옵션 품목 수량을 보정합니다.
  - 옵션 응답 `reasoning_metrics`에 `보정 전/후 주문량`, `최근 7일 판매량`, `판매 추세`, `재고 커버리지`, `유통기한 고위험`, `최종 보정계수`를 추가합니다.
- AI 주문 근거 생성 입력을 실데이터 기반으로 보강했습니다.
  - `/management/ordering/recommend` 호출 시 `current_context.option_summaries`를 함께 전달해 Gemini 근거가 하드코딩 문구가 아닌 실측 지표를 사용하도록 변경했습니다.
  - 최종 주문 옵션 병합 시 Gemini는 `reasoning_text`(근거 문장)만 반영하고, 수량/항목/지표/추천여부는 백엔드 하이브리드 계산 결과를 고정 사용합니다.
- 주문 추천 코드 리팩토링을 진행했습니다.
  - `OrderingRepository`의 판매/재고 지표 조회 필터 조립 로직을 공통 helper로 통합했습니다.
  - 하이브리드 보정계수 임계값을 클래스 상수로 분리해 튜닝 포인트를 명확히 했습니다.

- Docker `load` 단계 실패 이슈를 보완했습니다.
  - 기존 DB에서 `store_clusters`가 구스키마여도 동작하도록 `0021_backfill_store_clusters_columns.sql` 마이그레이션을 추가했습니다.

- 프론트 주문관리에서 mock 마감데이터를 제거하고 backend 실데이터(`options/deadline`)를 직접 표기하도록 연동되었습니다.

- `resource/06. 유통기한 및 납품일/*.xlsx`를 기존 적재 규약과 동일하게 DB 파이프라인에 추가했습니다.
  - migration 추가: `0019_create_order_arrival_schedule.sql`, `0020_create_product_shelf_life.sql`
  - 신규 raw 테이블: `raw_order_arrival_schedule`, `raw_order_arrival_reference`, `raw_product_shelf_life`, `raw_product_shelf_life_group_reference`
  - manifest dataset: direct load 4건 + workbook 보존 2건
- 주문/생산 기초 조회에 신규 raw 데이터를 연결했습니다.
  - `OrderingService.get_deadline`: AI 응답 부재 시 `raw_order_arrival_schedule` 마감시간을 우선 사용
  - `ProductionService`: `raw_product_shelf_life` 유통기한 값을 우선 사용하고 미존재 시 기존 키워드 규칙 fallback
- 폐기손실/재고현황/주문관리/발주이력에서 가설 기반 설명을 실데이터 기준으로 강화했습니다.
  - `폐기손실`, `재고현황`: SKU 유통기한(`raw_product_shelf_life`) 우선 조회
  - `주문관리(/api/ordering/options)`: 옵션 아이템 note에 마감/도착/유통기한 정보를 주입
  - `발주이력(/api/ordering/history)`: explainability 근거에 납품/유통기한 데이터 소스를 명시
- 주문 옵션 응답 계약과 스케줄 매핑 안정성을 보강했습니다.
  - `OrderingOptionsResponse`에 `deadline_items`를 명시하고 `list_options`에서 항상 생성해 반환
  - `get_order_arrival_schedule_map`이 SKU별 대표 row를 `hit_count DESC` + 시간 정렬로 결정하도록 보강

## 최근 업데이트 (2026-04-23)

- `docs/golden-queries-new.csv`를 `일반화 쿼리`/`예시 쿼리` 분리 컬럼으로 재작성했습니다.
  - `일반화 쿼리`: `:store_id`, `:start_date`, `:end_date` 파라미터 기준 템플릿
  - `예시 쿼리`: `POC_010` + 기간 실값 기준 실행 예시

- `docs/design-docs.md`에 본사 시연자/점주 실사용자 이중 타깃 관점이 반영되었습니다.
  - 이번 세션의 백엔드 API/스키마 변경은 없고, 문서 관점 정렬만 수행했습니다.

- `docs/design-docs.md`가 실제 프론트 라우터 기준 페이지 전략 문서로 정비되었습니다.
  - 이번 세션의 백엔드 API/스키마 변경은 없고, 화면 목적/메시지/기대행동 문서 정렬만 반영했습니다.

- 점주 골든쿼리 데이터셋 문서(`docs/golden-queries-store-owner.csv`)를 추가했습니다.
  - 이번 세션의 백엔드 API/스키마 코드 변경은 없고, 기존 마이그레이션 기준 테이블/컬럼으로 실행 쿼리 예시를 구성했습니다.
- 점주 골든쿼리 문서에 200건을 추가해 총 400건으로 확장했습니다.
  - 추가분도 기존 DB 스키마(raw/core/운영 테이블) 기준 SQL 템플릿으로 구성했습니다.
- `br-korea-poc-backend/docs/golden-queries-store-owner.csv`에서 연결형 질문은 `질문번호`를 `그룹번호-순번-` 형식(예: `067-003-`)으로 표기하도록 정리했습니다.
- 같은 CSV에 기준일시 `2026-03-05 09:00 (KST)` 컬럼을 추가하고, 각 실제 쿼리/예상 답변에 기준일시 문구를 반영했습니다.

- 프론트 사이드바 상단 `AgentGo Biz` 로고 클릭 동선이 대문(`/`)으로 변경되었습니다.
  - 이번 세션에서 백엔드 API/스키마 코드는 변경하지 않았습니다.
- 프론트 `/settings` 화면 셸이 `Settings v3` 원본 HTML 구조로 정렬되었습니다.
  - 이번 세션에서 백엔드 API/스키마 코드는 추가 변경하지 않았습니다.
- 프론트 `/settings` 내부 패널/모달이 `Settings v3` 원본 마크업 기준으로 재작성되었습니다.
  - 이번 세션에서 백엔드 API/스키마 코드는 변경하지 않았습니다.
- 프론트 settings 코드가 `VIBE_CODING_GUIDE` 기준으로 로직 분리(hooks/mockdata) 리팩토링되었습니다.
  - 이번 세션에서 백엔드 API/스키마 코드는 변경하지 않았습니다.
- 프론트 `/settings` 스타일 파일이 feature 전용 CSS에서 전역 스타일 엔트리로 통합되었습니다.
  - 이번 세션에서 백엔드 API/스키마 코드는 변경하지 않았습니다.
- 프론트 `/settings` 추가 패널(`Agents/Connectors/RBAC`) 인라인 스타일 정리가 진행되었습니다.
  - 이번 세션에서 백엔드 API/스키마 코드는 변경하지 않았습니다.
- 프론트 `/settings` 전체 화면 비율 보정(AppLayout padding 해제 + settings 셸 full-viewport)이 반영되었습니다.
  - 이번 세션에서 백엔드 코드/계약 변경은 없습니다.

## 최근 업데이트 (2026-04-22)

- Plan 구현: explainability 병렬 보강 + 기준일시 실사용
  - `app/schemas/explainability.py`, `app/services/explainability_service.py`를 추가했습니다.
  - `GET /api/explainability/{trace_id}` 엔드포인트를 추가했습니다.
  - sales/ordering/production/analytics/notifications 주요 응답에 `explainability` 필드를 확장했습니다.
  - `X-Reference-Datetime`를 실제 조회 기본값으로 반영했습니다.
    - sales/analytics: `date_from/date_to` 기본값
    - production: `business_date` 기본값
    - ordering: 마감 계산 기준 시각

- CORS 허용 헤더를 확장했습니다.
  - 프론트가 전달하는 `X-Store-Id`, `X-Reference-Datetime`를 허용해 기준 점포/기준 일시 헤더가 preflight에서 차단되지 않도록 정비했습니다.

- 본사 Settings v3 UI 개편 연계
  - 이번 세션의 코드 변경은 프론트(`/settings`) 화면 개편 중심이며, 백엔드 API 계약 변경은 없습니다.
  - 기존 설정/감사 로그 API(`GET/PUT /api/settings/prompt`, `GET /api/audit/logs`)를 동일하게 사용합니다.

- QA 운영 자산 동기화
  - 기준 QA 마스터 참조 파일을 `../docs/reference/qa-master.csv`로 추가했습니다.
  - QA 실행 이력 기록 도구 `../docs/qa/qa-run-log.py`를 기준 경로로 문서화했습니다.

- QA 안정화 패치 반영
  - `AuditService`가 저장소 비가용 시에도 감사 로그 안전 payload를 생성해 요청 플로우를 중단하지 않도록 보강했습니다.
  - `SalesService.list_prompts`는 DB/AI 장애 시에도 기본 추천 질문 10건을 반환하도록 폴백을 추가했습니다.
  - `tests/test_system_integration.py`의 AI import stub 계약을 최신 라우터 의존성(`get_*`, `APP_ENV`, ordering 응답 스키마)에 맞게 정비했습니다.

- `/api/analytics/market-intelligence/insights` no-fallback 정책 반영
  - 상권 인사이트 fallback 응답을 제거하고, AI 생성 실패 시 오류를 반환합니다.
  - `MarketInsightsResponse.source`를 `"ai"` 단일 값으로 고정했습니다.
  - 인사이트 캐시를 추가해 캐시 hit 시 즉시 반환 후 백그라운드 refresh를 수행합니다.

- `/api/sales/*` 서술형 RAG+Gemini 강화
  - `/api/sales/prompts`는 AI 추천 질문 경로를 강제하고 실패 시 오류를 반환합니다.
  - `/api/sales/insights`는 실데이터 지표 기반으로 섹션 summary를 AI로 재생성합니다.
  - `/api/sales/campaign-effect`는 실데이터 기반 summary/actions를 AI로 생성합니다.

- AI 연동 fail-fast 정책 보강
  - 상권 인사이트 AI 호출 timeout을 `60s → 20s`로 단축했습니다.

- 이번 세션의 생산 예측 모델 우선 적용은 AI 서비스 `/predict` 내부 변경이며 backend API 코드는 변경하지 않았습니다.

- 이번 세션의 `/production/status` 주문 마감 시간 표기 보정은 frontend 레이어 변경이며 backend API 코드는 변경하지 않았습니다.

- `/api/ordering/history/insights`를 AI 기반으로 전환했습니다.
  - 백엔드는 주문 이력 원천 + 집계 요약을 AI 서비스로 전달하고, AI 응답을 기존 화면 계약(`kpis/anomalies/top_changed_items`)으로 반환합니다.
  - 응답 메타 필드 `sources`, `retrieved_contexts`, `confidence`를 추가했습니다.
  - AI 생성 실패/비정상 payload는 `502 AI ordering insights generation failed`로 반환합니다.

- `/api/sales/summary` fallback 제거
  - DB 엔진 미구성/데이터 없음/쿼리 오류 시 0값 기본 응답 대신 오류를 반환하도록 정비했습니다.
- `/api/sales/insights` fallback 제거
  - no-data 섹션 자동 생성을 제거하고, 핵심 인사이트 실데이터가 부족하면 오류를 반환합니다.
- `/api/sales/campaign-effect` fallback 제거
  - 캠페인 컨텍스트가 없을 때 빈 구조를 반환하지 않고 오류를 반환합니다.
- `sales` 엔드포인트(`summary/insights/campaign-effect`)에 422/404/500 예외 매핑을 추가했습니다.

- 이번 세션의 프론트 빌드 오류 복구 작업은 frontend 레이어 변경이며 backend API 코드 변경은 없습니다.

- `/api/analytics/metrics` fallback 제거
  - 잘못된 `store_id`를 전체 점포 집계로 대체하지 않고 422 오류를 반환하도록 변경했습니다.
  - DB 엔진 미구성 시 빈 배열 fallback 대신 500 오류를 반환합니다.
- `/api/analytics/sales-trend` fallback 제거
  - DB 엔진 미구성/쿼리 실패 시 빈 구조 응답 대신 오류를 반환하도록 변경했습니다.

- 생산 화면 응답 지연 완화를 위해 `GET /api/production/waste-summary`, `GET /api/production/inventory-status`의 AI 근거 요약 대기시간을 제한했습니다.
  - AI 요약 생성은 1.2초 내 완료 시에만 포함하고, 시간 초과/오류 시 기본 근거(`evidence`)로 즉시 응답합니다.
- 두 API의 서비스 캐시 TTL을 `45초 → 300초`로 상향해 동일 점포 재조회 시 응답 지연을 줄였습니다.

- 발주 이력 점포 검증 로직을 보완했습니다.
  - `raw_store_master`에 점포가 없더라도 `raw_order_extract`에 주문 데이터가 있으면 유효 점포로 인정합니다.
- `GET /api/production/inventory-status` 반환값 방어 로직을 추가했습니다.
  - 레포지토리에서 레거시 2-튜플(`rows`, `total_items`)을 반환해도 서비스가 3-튜플 형태로 정규화해 `not enough values to unpack` 오류를 방지합니다.
- 생산 이미지 경로 호환성 이슈 대응을 위해 프론트에서 `image_url` 포맷(`images/...`, `/images/...`, 절대/상대경로)을 정규화하도록 보완했습니다.
- `GET /api/production/inventory-status` 422 방어를 위해 요약 지표(`shortage/excess/normal`)를 안전 정수 변환으로 처리했습니다.
- `GET /api/production/inventory-status`가 `page`, `page_size` 쿼리 파라미터를 공식 지원하도록 엔드포인트를 정비했습니다.
- `GET /api/production/inventory-status`
  - `store_id`를 필수 파라미터로 강제하고, 데이터 없음(404) / 요청오류(422) 분기를 명시했습니다.
  - `core_stock_rate`, `core_stockout_time` 기반 재고율 진단 지표와 `evidence`를 응답에 포함하도록 확장했습니다.
- `GET /api/production/waste-summary`
  - D+1 보정 로스(`당일 잉여 - 익일 흡수`)와 실폐기(`disuse_qty`)를 분리 집계하도록 변경했습니다.
  - 품목군 키워드 기반 가설 유통기한(1일/2일/0일)과 추정 만기 리스크를 응답에 포함했습니다.

## Tech Stack

| 패키지 | 버전 |
|---|---|
| Python | 3.10 |
| FastAPI | 0.115.0 |
| uvicorn | 0.30.6 |
| Pydantic | 2.9.2 |
| pydantic-settings | 2.5.2 |
| SQLAlchemy | (sync, psycopg2-binary) |
| PostgreSQL | 16 |
| openpyxl | xlsx 적재용 |
| httpx | AI 서비스 연동 |

## Directory Structure

```text
br-korea-poc-backend/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   ├── analytics.py        # GET /api/analytics/metrics
│   │       │   ├── audit.py            # GET /api/audit/logs
│   │       │   ├── bootstrap.py        # GET /api/bootstrap
│   │       │   ├── channels.py         # GET /api/channels/drafts
│   │       │   ├── data_catalog.py     # GET /api/data/catalog, /api/data/preview/{table}
│   │       │   ├── explainability.py   # GET /api/explainability/{trace_id}
│   │       │   ├── health.py           # GET /api/health
│   │       │   ├── home.py             # GET /api/home/schedule
│   │       │   ├── hq.py               # GET /api/hq/coaching, /api/hq/inspection
│   │       │   ├── notifications.py    # GET /api/notifications
│   │       │   ├── ordering.py         # 주문 옵션/선택/이력/요약
│   │       │   ├── production.py       # 생산 현황/등록/이력/요약
│   │       │   ├── review.py           # GET /api/review/checklist
│   │       │   ├── sales.py            # 매출 프롬프트/질의/인사이트
│   │       │   ├── signals.py          # GET /api/signals
│   │       │   └── simulation.py       # POST /api/simulation/preview
│   │       └── router.py               # /api 라우터 묶음
│   ├── core/
│   │   ├── auth.py                     # X-User-Role 헤더 기반 역할 식별
│   │   ├── config.py                   # 환경 변수 설정 (pydantic-settings)
│   │   ├── deps.py                     # FastAPI 의존성 주입 함수
│   │   └── reference_datetime.py       # 기준 일시 파서/기본 기간 해석
│   ├── infrastructure/
│   │   └── db/
│   │       └── connection.py           # SQLAlchemy 엔진 싱글턴
│   ├── models/                         # (현재 미사용, 확장 예정)
│   ├── repositories/
│   │   ├── analytics_repository.py
│   │   ├── audit_repository.py
│   │   ├── bootstrap_repository.py
│   │   ├── data_catalog_repository.py
│   │   ├── notifications_repository.py
│   │   ├── ordering_repository.py
│   │   ├── production_repository.py
│   │   ├── sales_repository.py
│   │   └── signals_repository.py
│   ├── schemas/
│   │   ├── analytics.py
│   │   ├── audit.py
│   │   ├── bootstrap.py
│   │   ├── channels.py
│   │   ├── contracts.py
│   │   ├── data_catalog.py
│   │   ├── db_schemas.py               # raw/core 테이블 Pydantic 모델
│   │   ├── explainability.py
│   │   ├── notifications.py
│   │   ├── home.py
│   │   ├── ordering.py
│   │   ├── production.py
│   │   ├── review.py
│   │   ├── sales.py
│   │   ├── signals.py
│   │   └── simulation.py
│   ├── services/
│   │   ├── ai_client.py                # AI 서비스 HTTP 클라이언트
│   │   ├── analytics_service.py
│   │   ├── audit_service.py
│   │   ├── bootstrap_service.py
│   │   ├── data_catalog_service.py
│   │   ├── explainability_service.py   # explainability payload 캐시/생성
│   │   ├── notifications_service.py
│   │   ├── home_service.py
│   │   ├── ordering_service.py
│   │   ├── planning_service.py
│   │   ├── production_service.py
│   │   ├── sales_service.py            # 민감정보 필터링, AI 라우팅 포함
│   │   └── signals_service.py
│   └── main.py                         # FastAPI 앱 엔트리포인트
├── db/
│   ├── manifests/
│   │   └── resource_load_manifest.json # 원본 파일 → raw 테이블 매핑
│   └── migrations/
│       ├── 0001_create_raw_resource_tables.sql
│       ├── 0002_create_core_views.sql
│       ├── 0003_create_operational_tables.sql
│       ├── 0004_add_store_id_to_operational_tables.sql
│       ├── 0005_create_new_workbook_raw_tables.sql
│       ├── 0006_create_campaign_raw_tables.sql
│       ├── 0007_create_settlement_and_telecom_raw_tables.sql
│       ├── 0011_create_raw_weather_daily.sql
│       └── 0012_create_raw_seoul_market_reference.sql
├── scripts/
│   ├── migrate_db.py                   # SQL 마이그레이션 실행
│   ├── load_resource_to_db.py          # resource 파일 → DB 적재
│   ├── load_weather_data.py            # Open-Meteo 일별 기온/강수 적재
│   ├── load_market_reference_data.py   # reference 상권 추정매출/유동인구 CSV 적재
│   ├── inspect_resource_db.py          # 적재 상태 조회
│   └── test_ai_client_integration.py   # AI 클라이언트 통합 테스트
├── tests/
│   ├── test_health.py
│   ├── test_ordering_history_insights_ai.py
│   └── test_system_integration.py      # backend ↔ AI contract/system integration
├── Dockerfile
├── docker-compose.yml                  # PostgreSQL 컨테이너 (포트 5435)
├── environment.yml
└── requirements.txt
```

## 환경 변수

`.env` 파일 또는 환경 변수로 주입합니다.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://postgres:postgres@localhost:5435/br_korea_poc` | PostgreSQL 연결 URL |
| `EXTERNAL_API_KEY` | (빈 값) | 공공데이터포털 API 키 (소진공 SmallShop 실시간 조회용, 우선 사용) |
| `SBIZ_API_SNS_ANALYSIS_KEY` | (빈 값) | 소진공 빅데이터 오픈API `SNS 분석` 인증키 |
| `SBIZ_API_STARTUP_WEATHER_KEY` | (빈 값) | 소진공 빅데이터 오픈API `창업기상도` 인증키 |
| `SBIZ_API_HOTPLACE_KEY` | (빈 값) | 소진공 빅데이터 오픈API `핫플레이스` 인증키 |
| `SBIZ_API_SALES_INDEX_KEY` | (빈 값) | 소진공 빅데이터 오픈API `점포당 매출액 추이` 인증키 |
| `SBIZ_API_BUSINESS_DURATION_KEY` | (빈 값) | 소진공 빅데이터 오픈API `업력현황` 인증키 |
| `SBIZ_API_STORE_STATUS_KEY` | (빈 값) | 소진공 빅데이터 오픈API `업소현황` 인증키 |
| `SBIZ_API_COMMERCIAL_MAP_KEY` | (빈 값) | 소진공 빅데이터 오픈API `상권지도` 인증키 |
| `SBIZ_API_DETAIL_ANALYSIS_KEY` | (빈 값) | 소진공 빅데이터 오픈API `상세분석` 인증키 |
| `SBIZ_API_DELIVERY_ANALYSIS_KEY` | (빈 값) | 소진공 빅데이터 오픈API `배달분석` 인증키 |
| `SBIZ_API_TOUR_FESTIVAL_KEY` | (빈 값) | 소진공 빅데이터 오픈API `관광 축제 정보` 인증키 |
| `SBIZ_API_SIMPLE_ANALYSIS_KEY` | (빈 값) | 소진공 빅데이터 오픈API `간단분석` 인증키 |
| `AI_SERVICE_URL` | `http://localhost:6001` | AI 서비스 URL (미설정 시 repository 경로로 처리) |
| `AI_SERVICE_TOKEN` | (빈 값) | AI 서비스 인증 토큰 |
| `CORS_ORIGINS` | `http://localhost:5173,http://localhost:6003` | 허용 Origin (쉼표 구분) |
| `APP_ENV` | `local` | 실행 환경 |
| `APP_PORT` | `8000` | 내부 기본 포트 |

- 소진공 빅데이터 OpenAPI 키는 인증키(`certKey`) 값만 `.env`에 저장하고, URL 전체(`...?certKey=...`)는 코드/문서에 직접 하드코딩하지 않습니다.
- `GET /api/analytics/market-intelligence`의 실시간 경쟁사 조회 키 우선순위:
- `GET /api/analytics/market-intelligence` 응답의 `store_reports`는 소진공 11개 API 키(`SBIZ_API_*`) 설정 상태를 기준으로 동적으로 생성되며, 경쟁사 실시간 연동에 사용된 키는 `연동중`으로 표시됩니다.
- `SBIZ_API_SALES_INDEX_KEY`가 설정된 경우 `market-intelligence`는 소진공 `점포당 매출액 추이(slsIdex)` 조회를 추가 시도합니다. 성공 시 추정 매출 요약을 보정하고, 실패 시 `store_reports` 상태를 `점검 필요`로 표시합니다.
- `store_reports`의 기본 상태는 `실호출 미확인`이며, 실제 호출이 성공한 API만 `연동중`으로 표시합니다.
  - 1순위: `EXTERNAL_API_KEY` (공공데이터포털 SmallShop)
  - 2순위: `SBIZ_API_COMMERCIAL_MAP_KEY` (소진공 상권지도 certKey)
  - 3순위: `SBIZ_API_STORE_STATUS_KEY` (소진공 업소현황 certKey, `/sbiz/api/bizonSttus/storSttus/search.json` 기반 대체 조회)

## 실행

```bash
# PostgreSQL 컨테이너 기동 (포트 5435)
open -a Docker
docker compose up -d postgres

# 의존성 설치
pip install -r requirements.txt

# 1) DB 스키마 생성
python scripts/migrate_db.py

# 2) resource 데이터 적재
python scripts/load_resource_to_db.py

# 3) 외부 날씨 데이터 적재(Open-Meteo)
python scripts/load_weather_data.py --start-date 2026-01-01 --end-date 2026-04-19

# 4) 상권 reference 데이터 적재 (서울시 우리마을가게 CSV)
python scripts/load_market_reference_data.py

# 5) 적재 상태 확인
python scripts/inspect_resource_db.py

# 6) 개발 서버 실행
uvicorn app.main:app --reload --port 6002
```

### 실행 순서

1. `python scripts/migrate_db.py`
   `db/migrations` 아래 SQL을 적용해 raw/core/운영 테이블과 뷰를 생성합니다.
2. `python scripts/load_resource_to_db.py`
   `db/manifests/resource_load_manifest.json` 기준으로 `resource/` 파일을 raw 테이블에 적재합니다.
3. `python scripts/load_weather_data.py`
   Open-Meteo Archive API에서 시도별 일평균 기온/강수량을 수집해 `raw_weather_daily`에 upsert 합니다.
4. `python scripts/load_market_reference_data.py`
   `reference/dss_eda_seoul_market_area-main/data`의 추정매출/추정유동인구 CSV를
   `raw_seoul_market_sales`, `raw_seoul_market_floating_population`에 적재합니다.
5. `python scripts/inspect_resource_db.py`
   현재 DB에 적재된 raw/운영 테이블 row count와 상태를 확인합니다.
6. `uvicorn app.main:app --reload`
   FastAPI 개발 서버를 실행합니다.

- Swagger UI: `http://localhost:6002/docs`
- Redoc: `http://localhost:6002/redoc`

### Docker 빌드 (단독 컨테이너)

```bash
docker build -t br-korea-poc-backend .
docker run -p 6002:6002 --env-file .env br-korea-poc-backend
```

## 코드 컨벤션 (ruff / black / mypy)

```bash
# Lint
ruff check .

# Format
black .

# Type check
mypy .
```

## Session Update (2026-04-21, Backend-AI Interface)

- `AIServiceClient`에 `X-Request-Id` 헤더 자동 전송을 추가하고, AI 에러 계약(`error_code/message/retryable/trace_id`) 파싱 로깅을 반영했습니다.
- 매출 질의 계약에서 `store_id`를 필수로 정비했습니다. (`SalesQueryRequest.store_id` required)
- AI 주문 마감 알림 batch 인터페이스를 호출할 수 있도록 `get_ordering_deadline_alerts_batch()`를 추가했습니다.
- AI 계약 버전 확인을 위해 `get_contract_info()` 클라이언트 메서드를 추가했습니다.

## Session Update (2026-04-21, Role-Based Market Insights)

- 상권 인사이트 API를 추가했습니다.
  - 점주: `GET /api/analytics/market-intelligence/insights`
  - 본사: `GET /api/analytics/market-intelligence/insights/hq` (`hq_admin`, `hq_operator`)
- `AnalyticsService`가 AI 서비스(`POST /analytics/market/insights`)에 상권 집계 데이터를 전달해 실행형 인사이트를 생성하도록 확장했습니다.
- weekly-report 다운로드는 AI 인사이트의 markdown을 우선 사용하고, 실패 시 기존 템플릿으로 fallback합니다.

`mypy`는 `pyproject.toml` 설정에 따라 `tests/`, `scripts/` 디렉터리를 제외하고 검사합니다.

## API 엔드포인트

| 경로 | 설명 |
|---|---|
| `GET /health` | 서버 헬스체크 |
| `GET /api/health` | API 라우터 헬스체크 |
| `GET /api/bootstrap` | 앱 초기화 데이터 |
| `GET /api/home/schedule` | 홈 일정 패널 |
| `GET /api/dashboard/notices` | 대시보드 공지 |
| `GET /api/dashboard/alerts` | 대시보드 경고 요약 |
| `GET /api/dashboard/summary-cards` | 대시보드 요약 카드 |
| `GET /api/data/catalog` | raw/core 테이블 목록 |
| `GET /api/data/preview/{table_name}` | 테이블 미리보기 |
| `GET /api/analytics/metrics` | 매출 현황 화면 KPI 7종(투고 건수·배달 매출액·매장 결제 건수·커피 동반 구매율·런치/스윙/디너 시간대 평균 단가) + `selected_period_total_sales` |
| `GET /api/analytics/market-intelligence` | 상권 인텔리전스(구/동/업종/연도/분기/반경 스코프 지원, `EXTERNAL_API_KEY` 설정 시 소진공 SmallShop 실시간 경쟁사 반경 조회 포함) |
| `GET /api/analytics/market-intelligence/weekly-report` | 상권 인텔리전스 데이터를 기반으로 상권 인텔리전스 데이터를 기반으로 주간 분석 리포트를 PDF/markdown으로 다운로드한다. |
| `GET /api/analytics/weather-impact` | 날씨(기온/강수)와 매출 지표 상관 분석 |
| `GET /api/signals` | 매출 시그널 목록 |
| `GET /api/notifications` | 알림 인박스 |
| `GET /api/audit/logs` | 감사 로그 조회 |
| `GET /api/ordering/options` | 주문 추천 옵션 |
| `GET /api/ordering/context/{notification_id}` | 알림 기반 주문 컨텍스트 |
| `GET /api/ordering/alerts` | 주문 알림 |
| `POST /api/ordering/selections` | 주문 선택 저장 |
| `GET /api/ordering/selections/history` | 주문 선택 이력 |
| `GET /api/ordering/selections/summary` | 주문 요약 |
| `GET /api/production/overview` | 생산 현황 |
| `GET /api/production/items` | 생산 SKU 목록 (front 계약 기준) |
| `GET /api/production/items/{sku_id}` | 생산 SKU 상세 (front 계약 기준) |
| `GET /api/production/skus` | 생산 SKU 목록 legacy alias |
| `GET /api/production/alerts` | 생산 알림 |
| `POST /api/production/registrations` | 생산 등록 |
| `GET /api/production/registrations/history` | 생산 등록 이력 |
| `GET /api/production/registrations/summary` | 생산 요약 |
| `GET /api/sales/prompts` | 매출 추천 질문 목록 |
| `POST /api/sales/query` | 매출 자연어 질의 |
| `GET /api/sales/insights` | 매출 구조화 인사이트 |
| `GET /api/channels/drafts` | 채널 초안 |
| `GET /api/review/checklist` | 리뷰 체크리스트 |
| `POST /api/simulation/preview` | 시뮬레이션 미리보기 |
| `POST /api/production/simulation` | 생산 시뮬레이션 리포트 |
| `POST /api/v1/production/simulation` | 생산 시뮬레이션 리포트 alias |
| `GET /api/hq/coaching` | 본사 주문 코칭 |
| `GET /api/hq/inspection` | 본사 생산 점검 |

### 홈 대시보드 응답 메모

- `priority_actions[].ai_reasoning`: AI 추천 상세 근거
- `priority_actions[].confidence_score`: AI 신뢰도 점수
- `priority_actions[].is_finished_good`: 본사 납품 완제품 여부
- `cards[].delivery_scheduled`: 주문 관리 카드의 배송 예정 여부
- 생산 시뮬레이션은 `/api/production/simulation`과 `/api/v1/production/simulation`을 모두 지원합니다.
- `tests/test_system_integration.py`는 backend 홈 응답 구조와 AI FastAPI 시뮬레이션 계약을 인메모리로 검증합니다.
- HQ 코칭/점검 API는 `core_store_master`, `ordering_selections`, `production_registrations`를 기반으로 매장별 최신 운영 데이터를 조합해 응답합니다.

## Front 기준 계약 메모

백엔드는 프론트가 기대하는 경로와 응답 shape를 기준으로 유지합니다.

### Production

- `GET /api/production/overview`
  - 주요 필드: `updated_at`, `refresh_interval_minutes`, `summary_stats`, `alerts`
- `GET /api/production/items`
  - query: `page`, `page_size`, `store_id`
- `GET /api/production/items/{sku_id}`
  - query: `store_id`
- `POST /api/production/registrations`

`overview` 응답에는 내부 호환을 위해 `production_lead_time_minutes`, `danger_count`, `items`도 함께 유지합니다.

### Sales

- `POST /api/sales/query`는 프론트 응답 기준으로 `text`, `evidence`, `actions`, `processing_route`, `blocked` 등을 반환합니다.
- AI 서비스 응답이 다른 shape여도 `services/ai_client.py`에서 프론트 계약으로 변환합니다.
- repository 처리 경로명은 프론트 표시 규칙에 맞춰 `repository`를 사용합니다.

### Audit

- `/api/audit/logs`는 프론트 현재 동작 기준으로 기본 접근을 허용합니다.

## 데이터 적재 파이프라인

원본 파일은 상위 `resource/` 디렉터리에 유지하며, 적재 대상·매핑은 `db/manifests/resource_load_manifest.json`에서 관리합니다.

- `db/migrations`: 테이블/뷰 생성
- `db/manifests/resource_load_manifest.json`: `resource` 파일과 raw 테이블 매핑
- `scripts/load_resource_to_db.py`: manifest 기준 적재 실행
- `scripts/inspect_resource_db.py`: 적재 결과 조회

```bash
# 이미 스키마가 생성된 상태에서 적재 실행
python scripts/load_resource_to_db.py

# 적재 상태 확인 (전체 테이블 row count)
python scripts/inspect_resource_db.py
```

### 로더 종류

| loader | 설명 |
|---|---|
| `csv` | UTF-8/CP949 자동 감지 CSV |
| `xlsx` | openpyxl 단일 시트 적재 |
| `workbook_rows` | 멀티 시트 전체를 `raw_workbook_rows`에 JSON 행으로 보존 |

### Resource Mapping

| resource 파일/폴더 | manifest dataset | migration/raw 테이블 | 비고 |
|---|---|---|---|
| `resource/03. 결제 수단 코드/결제 할인 수단 코드 테이블.csv` | `pay_code_csv` | `raw_pay_cd` | 코드 마스터 적재 |
| `resource/04. POC 대상 데이터_제공데이터/01. 점포 마스터/던킨+점포마스터_매핑용.xlsx` | `store_master` | `raw_store_master` | 점포 기준 정보 |
| `resource/04. POC 대상 데이터_제공데이터/02. 매출/01. 일자별 시간대별 상품별 매출 데이터/*.xlsx` | `daily_store_item_tmzon` | `raw_daily_store_item_tmzon` | 6개 파일 direct load |
| `resource/04. POC 대상 데이터_제공데이터/02. 매출/02. 일자별 시간대별 캠페인 매출 데이터/일자별+시간대별+캠페인+매출.xlsx` | `daily_store_cpi_tmzon` | `raw_daily_store_cpi_tmzon` | 캠페인 시간대 매출 |
| `resource/04. POC 대상 데이터_제공데이터/02. 매출/03. 일자별 결제 수단별 매출 데이터/일자별+결제+수단별+매출.xlsx` | `daily_store_pay_way` | `raw_daily_store_pay_way` | 결제수단 매출 |
| `resource/04. POC 대상 데이터_제공데이터/02. 매출/04. 일자별 상품별 매출/일자별+상품별+매출.xlsx` | `daily_store_item` | `raw_daily_store_item` | 상품 매출 |
| `resource/04. POC 대상 데이터_제공데이터/02. 매출/05. 일자별 온_오프라인 구분/*.xlsx` | `daily_store_online` | `raw_daily_store_online` | 온라인/오프라인 매출 |
| `resource/04. POC 대상 데이터_제공데이터/04. 생산/생산 데이터 추출.xlsx` | `production_extract` | `raw_production_extract` | `Sheet1` direct load |
| `resource/04. POC 대상 데이터_제공데이터/05. 주문/주문+데이터.xlsx` | `order_extract` | `raw_order_extract` | `Sheet1` direct load |
| `resource/04. POC 대상 데이터_제공데이터/05. 주문/주문+데이터.xlsx` | `order_workbook_rows` | `raw_workbook_rows` | 보조 시트 포함 workbook 보존 |
| `resource/04. POC 대상 데이터_제공데이터/06. 재고/재고+데이터+추출.xlsx` | `inventory_extract` | `raw_inventory_extract` | `Sheet1` direct load |
| `resource/04. POC 대상 데이터_제공데이터/07. 정산 기준 정보/정산+기준정보+마스터+테이블+추출.xlsx` | `settlement_master_extract` | `raw_settlement_master` | `Sheet1` direct load |
| `resource/04. POC 대상 데이터_제공데이터/07. 정산 기준 정보/정산+기준정보+마스터+테이블+추출.xlsx` | `settlement_master_workbook` | `raw_workbook_rows` | workbook 원본 보존 |
| `resource/04. POC 대상 데이터_제공데이터/08. 통신사 제휴 할인 마스터/통신사+제휴+할인마스터.xlsx` | `telecom_discount_type` | `raw_telecom_discount_type` | `결제_할인 수단 목록` 시트 direct load |
| `resource/04. POC 대상 데이터_제공데이터/08. 통신사 제휴 할인 마스터/통신사+제휴+할인마스터.xlsx` | `telecom_discount_policy` | `raw_telecom_discount_policy` | `제휴사 결제_할인 목록` 시트 direct load |
| `resource/04. POC 대상 데이터_제공데이터/08. 통신사 제휴 할인 마스터/통신사+제휴+할인마스터.xlsx` | `telecom_discount_item` | `raw_telecom_discount_item` | `제휴사 결제_할인 연결 상품 목록` 시트 direct load |
| `resource/04. POC 대상 데이터_제공데이터/08. 통신사 제휴 할인 마스터/통신사+제휴+할인마스터.xlsx` | `telecom_discount_master_workbook` | `raw_workbook_rows` | 메타 시트 포함 workbook 보존 |
| `resource/04. POC 대상 데이터_제공데이터/09. 캠페인 마스터/캠페인+마스터.xlsx` | `campaign_master` | `raw_campaign_master` | `CPI_MST` 시트 direct load |
| `resource/04. POC 대상 데이터_제공데이터/09. 캠페인 마스터/캠페인+마스터.xlsx` | `campaign_item_group` | `raw_campaign_item_group` | `CPI_ITEM_GRP_MNG` 시트 direct load |
| `resource/04. POC 대상 데이터_제공데이터/09. 캠페인 마스터/캠페인+마스터.xlsx` | `campaign_item` | `raw_campaign_item` | `CPI_ITEM_MNG` 시트 direct load |
| `resource/06. 유통기한 및 납품일 /order_arrival_schedule.xlsx` | `order_arrival_schedule` | `raw_order_arrival_schedule` | `order_arrival_schedule` 시트 direct load |
| `resource/06. 유통기한 및 납품일 /order_arrival_schedule.xlsx` | `order_arrival_reference` | `raw_order_arrival_reference` | `arrival_reference` 시트 direct load |
| `resource/06. 유통기한 및 납품일 /order_arrival_schedule.xlsx` | `order_arrival_schedule_workbook` | `raw_workbook_rows` | 메타 시트 포함 workbook 보존 |
| `resource/06. 유통기한 및 납품일 /product_shelf_life.xlsx` | `product_shelf_life` | `raw_product_shelf_life` | `sku_shelf_life` 시트 direct load |
| `resource/06. 유통기한 및 납품일 /product_shelf_life.xlsx` | `product_shelf_life_group_reference` | `raw_product_shelf_life_group_reference` | `group_reference` 시트 direct load |
| `resource/06. 유통기한 및 납품일 /product_shelf_life.xlsx` | `product_shelf_life_workbook` | `raw_workbook_rows` | 메타 시트 포함 workbook 보존 |
| `resource/04. POC 대상 데이터_제공데이터/00. 테이블 구조/*.xlsx` | - | - | 문서/참조용 파일, 적재 대상 아님 |
| `resource/04. POC 대상 데이터_제공데이터/ERD_V0.2.png` | - | - | ERD 이미지, 적재 대상 아님 |
| `resource/04. POC 대상 데이터_제공데이터/POC_TABLE_DDL.sql` | - | - | 참고용 DDL, 적재 대상 아님 |
| `resource/04. POC 대상 데이터_제공데이터/03. 결제 수단 코드/*.csv` 중 중복본 | - | - | 중복 코드 파일, 현재 manifest 비대상 |

### 적재 대상 테이블

| raw 테이블 | 현재 row 수 |
|---|---|
| `raw_pay_cd` | 506 |
| `raw_store_master` | 66 |
| `raw_daily_store_item_tmzon` | 5,591,560 |
| `raw_daily_store_item` | 1,403,844 |
| `raw_daily_store_online` | 364,948 |
| `raw_daily_store_pay_way` | 141,930 |
| `raw_daily_store_cpi_tmzon` | 1,106 |
| `raw_production_extract` | 45,528 |
| `raw_order_extract` | 481,821 |
| `raw_inventory_extract` | 540,182 |
| `raw_campaign_master` | 207 |
| `raw_campaign_item_group` | 283 |
| `raw_campaign_item` | 6,537 |
| `raw_settlement_master` | 37 |
| `raw_telecom_discount_type` | 20 |
| `raw_telecom_discount_policy` | 53 |
| `raw_telecom_discount_item` | 38 |
| `raw_order_arrival_schedule` | 적재 후 갱신 |
| `raw_order_arrival_reference` | 적재 후 갱신 |
| `raw_product_shelf_life` | 적재 후 갱신 |
| `raw_product_shelf_life_group_reference` | 적재 후 갱신 |
| `raw_workbook_rows` | 1,074,750 |

### DB 계층 구조

```
raw_*            원본 데이터를 그대로 TEXT 컬럼으로 보존
  └─ core_*      raw_* 위에 정의된 뷰 (타입 캐스팅, NULLIF 처리)
       └─ 운영 테이블  앱이 직접 쓰는 트랜잭션 테이블
```

**core 뷰**

| 뷰 | 원본 테이블 |
|---|---|
| `core_store_master` | `raw_store_master` |
| `core_daily_item_sales` | `raw_daily_store_item` |
| `core_hourly_item_sales` | `raw_daily_store_item_tmzon` |
| `core_channel_sales` | `raw_daily_store_online` |

**운영 테이블**

| 테이블 | 설명 |
|---|---|
| `ordering_selections` | 주문 선택 이력 |
| `production_registrations` | 생산 등록 이력 |
| `audit_logs` | 도메인별 감사 로그 |
| `ingestion_runs` | 적재 실행 이력 |
| `ingestion_files` | 적재 파일별 결과 |
| `schema_migrations` | 마이그레이션 적용 이력 |

## 현재 데이터 연결 상태

- **매출·분석**: `core_daily_item_sales`, `core_hourly_item_sales`, `core_channel_sales`, `core_store_master` 뷰에 연결됩니다. `analytics/metrics`는 여기에 더해 `raw_daily_store_pay_way`, `raw_settlement_master`, `raw_telecom_discount_policy`를 사용해 할인 결제 비중을 계산합니다.
- **주문·생산 이력**: `ordering_selections`, `production_registrations`, `audit_logs` 운영 테이블을 사용합니다.
- **신규 workbook 데이터**
  - 생산·주문·재고·캠페인: 정식 raw 테이블로 direct load 되었고, 주문/생산/매출 repository가 우선 참조합니다.
  - 정산 기준 정보: `raw_settlement_master` direct load와 `raw_workbook_rows` 보존 적재를 함께 사용하며, `sales/insights`의 결제·할인 인사이트 설명에 활용됩니다.
  - 통신사 제휴 할인 마스터: 데이터 시트는 정식 raw 테이블로 direct load 하고, 메타 시트 보존을 위해 workbook 전체는 `raw_workbook_rows`에도 남깁니다. 활성 제휴 할인 맥락은 `sales/insights`, `signals`, `analytics/metrics`에 반영됩니다.
  - 레거시 csv/xlsx 중복 적재본은 cleanup 이후 제거되었고, 현재 raw 테이블은 최신 manifest 기준 source만 유지합니다.

## 권한 모델

역할 식별은 `X-User-Role` HTTP 헤더를 사용합니다. 헤더가 없으면 `store_owner`로 처리됩니다.

| 역할 | 설명 |
|---|---|
| `store_owner` | 점포주 (기본값) |
| `hq_operator` | 본사 운영 |
| `hq_planner` | 본사 기획 |

- 감사 로그 조회 등 일부 엔드포인트는 `require_roles` 의존성으로 역할을 제한합니다.
- 매출 질의(`POST /api/sales/query`)는 민감 키워드 감지 시 `hq_operator` / `hq_planner` 외 역할을 차단하고 감사 로그를 기록합니다.

## AI 서비스 연동

`AI_SERVICE_URL`이 설정돼 있으면 `SalesService`가 `AIServiceClient`를 통해 AI 서비스로 질의를 프록시합니다. 미설정 시에도 backend repository가 적재 데이터 기준으로 응답을 계산합니다.

## Session Update (2026-04-20)

- `GET /api/analytics/market-intelligence/weekly-report` 생성 로직에서 `store_profile`이 없는 경우를 null-safe로 보정했습니다.
- 현재 스코프에 `gu/dong`이 없고 매장 정보 조회가 실패해도 markdown/PDF 리포트 다운로드가 500 없이 동작합니다.
- `app.main`에 `/static/menu-images` 정적 마운트를 추가해 `resource/05. 던킨도너츠 메뉴/*.png` 파일을 URL로 직접 서빙합니다.
- `production` 응답(`items`, `item-detail`)에 `image_url` 필드를 추가했고, 품목명 기반 매칭 실패 시 `null`을 반환하도록 처리했습니다.
- 프론트에서 기본 플레이스홀더를 사용하도록 연계되어, backend `image_url`이 없는 품목도 화면 깨짐 없이 표시됩니다.
- `notifications`, `dashboard/notices`, `dashboard/alerts`, `dashboard/summary-cards`, `production/waste-summary`, `production/inventory-status` 500 원인을 정비해 `http://localhost:6003` Origin 기준 CORS 정상 응답(ACAO 포함)을 확인했습니다.
- 주문 도메인에서 `GET /api/ordering/history` 필터(`date_from/date_to/item_nm/is_auto`)를 지원하도록 확장했습니다.
- `GET /api/ordering/history/insights`를 추가해 발주 이력 기반 KPI/이상징후/변동 품목 인사이트를 제공합니다.
- `GET /api/analytics/market-intelligence`의 합성 랜덤 데이터 응답을 제거하고, 상권/고객 분석 필드를 실데이터(서울시 상권 reference + 소진공 실호출) 기준으로만 생성하도록 변경했습니다.
- `household_composition_pie`, `estimated_residence_regions`를 실데이터 집계로 추가했습니다.
- 요청한 연도/분기에 데이터가 없을 때는 동일 스코프의 가용 실데이터(연도/분기 조건 제외)로 자동 폴백하고, 폴백도 없으면 빈값+안내 문구를 반환합니다.
- `market-intelligence` 응답에 `industry_analysis`, `sales_analysis`, `population_analysis`, `regional_status`, `customer_characteristics`를 추가해 상권/고객 분석 화면 5개 블록(업종/매출/인구/지역/고객특성)을 단일 API로 제공합니다.
- `5년 업력현황`, `직장/주거 인구`, `소득소비`, `교통접근지수`, `고객특성(성비/핵심 연령·시간대)`은 reference 데이터 기반 실집계 또는 프록시 집계로 계산합니다.
- `raw_daily_store_cpi_tmzon`의 `cpi_nm/bill_cnt` 실데이터에서 신규/재방문 키워드가 식별되면 `customer_characteristics.new_customer_ratio / regular_customer_ratio`를 자동 계산합니다(미식별 시 null 유지).
- 현재 샘플 데이터의 `cpi_nm/cpi_cd`는 프로모션 중심 코드로 확인되어 신규/재방문 식별값이 없어, 해당 비율은 `null` + `data_sources` 안내 문구로 반환됩니다.
- 추가로 `raw_daily_store_item/raw_daily_store_pay_way/raw_order_extract`에서 고객 식별 컬럼(`customer_id/member_id/phone_no/card_no` 등)이 발견되면 자동탐지로 신규/단골 비율을 계산하도록 템플릿을 확장했습니다.
- `GET /api/ordering/history`, `GET /api/ordering/history/insights`는 `store_id` 필수 정책으로 전환했고, 누락/오입력 시 4xx 에러를 반환하도록 정비했습니다.
- 주문 추천 옵션 응답(`GET /api/ordering/options`)은 AI 날씨 정보가 비어 있을 때 Open-Meteo 예보 API를 폴백으로 호출해 구조화된 `weather` 객체를 채웁니다.
- `GET /api/analytics/metrics`는 `store_id=STORE_DEMO` 또는 미존재 점포 ID 요청 시 전체 매장 집계로 자동 폴백하도록 보정해 KPI가 0값으로 고정되는 케이스를 줄였습니다.
- `GET /api/analytics/metrics`에서 사용자가 선택한 기간(`date_from/date_to`)에 데이터가 전혀 없으면, 자동으로 최근 가용 7일 구간으로 폴백해 0집계 고정 현상을 완화했습니다.
- 프론트 기본 환경값은 `VITE_DEFAULT_STORE_ID=POC_010`, `VITE_DEFAULT_REFERENCE_DATETIME=2026-03-05T09:00` 기준으로 동기화합니다.
- `할인 결제 비중`은 0.1% 미만의 소수값도 `0.03%` 형태로 표시하도록 포맷을 보정해, 실제 할인 결제 집계가 `0.0%`로만 보이는 표시 한계를 줄였습니다.

- 상권 화면의 글로벌 실패 문구는 메인 분석 API(`/api/analytics/market-intelligence`) 오류 기준으로만 노출되도록 프론트 표시 정책이 조정되었습니다(보조 API 오류와 분리).

- `AnalyticsService.get_market_intelligence()`에 예외 안전 처리를 추가해 repository 내부 오류가 발생해도 `/api/analytics/market-intelligence`는 200 + 기본 구조를 반환합니다.
- `tests/test_health.py`에 `market-intelligence` 응답 형태(200/status shape) 검증 테스트를 추가했습니다.

## Session Update (2026-04-21)

- 이번 세션의 핵심 수정은 AI 서비스(`br-korea-poc-ai`) 내부 안정화(생산 에이전트 구현/오케스트레이터 라우팅 정리)이며, 백엔드 코드 변경은 없습니다.

## Session Update (2026-04-21, Round 2)

- 이번 라운드의 변경은 AI 서비스(`br-korea-poc-ai`)의 오케스트레이션/예외처리/컨텍스트 전달 정비이며, 백엔드 코드 변경은 없습니다.

## Session Update (2026-04-21, Round 3)

- `EXTERNAL_API_KEY` 기본값을 `stub-key`에서 빈 값으로 변경해 실제 인증키 입력 기준으로만 외부 연동이 동작하도록 정리했습니다.
- 상권 인텔리전스 키 선택 로직에서 `stub-key` 센티넬 분기를 제거하고, 실키 존재 여부만으로 1순위 키를 선택하도록 단순화했습니다.
- 매출 질의 응답의 repository 처리 경로명을 `stub_repository` → `repository`로 변경해 실데이터 경로명을 명확화했습니다.

## Session Update (2026-04-21, Round 3)

- `AnalyticsRepository.get_metrics()`의 "선택 기간 데이터 없음 시 최근 7일 자동 폴백" 분기를 제거해, 요청 기간 기준으로만 지표를 조회하도록 정리했습니다.
- 상권/고객 분석 레퍼런스 payload 생성(`_build_reference_market_payload`)에서 연/분기 조건 자동 완화(period fallback) 재조회 로직을 제거했습니다.

## Session Update (2026-04-22)

- Docker 실행 시 backend 컨테이너가 메뉴 이미지 인덱스를 읽을 수 있도록 `/menu-images` 경로 후보를 추가했습니다.
- `docker-compose.yml`의 backend 서비스에 `./br-korea-poc-front/public/images:/menu-images:ro` 볼륨을 연결해 `/api/production/items`의 `image_url` 누락을 완화했습니다.

## Session Update (2026-04-23, sales insights partial fallback)

- `app/repositories/sales/insight_repository.py`에서 `peak_hours/channel_mix/payment_mix/menu_mix` 중 일부 섹션 데이터가 비어도 전체 `404`를 반환하지 않도록 처리했습니다.
- 누락 섹션은 `status="review"`와 `데이터 상태=부족` 메트릭을 포함한 안내 섹션으로 채워 `/api/sales/insights`가 부분 성공(200) 응답을 반환합니다.
- `raw_daily_store_channel` 집계 중 `SUM(text)` 타입 오류가 발생하는 환경에서도 해당 섹션만 점검 상태로 내려 화면 전체 실패를 방지합니다.

## Session Update (2026-04-23, signals/sidebar removal scope)

- 이번 라운드의 `signals` 페이지 제거 및 사이드바 항목 제거는 프론트엔드 라우팅/메뉴 작업입니다.
- 백엔드 API/스키마 코드는 변경하지 않았습니다.

## Session Update (2026-04-23, HQ-as-owner golden queries)

- `docs/golden-queries-hq-as-owner.csv`를 신규 추가해 본사(기획/전략/영업/슈퍼바이저/점장/상품팀) 관점의 점주 질의 200건을 정리했습니다.
- 컬럼은 `질문번호, 기준일시, 본사직무, 에이전트, 질문, 평가항목, 가용여부, 가용 데이터, 테이블/컬럼, 가정/갭, 실제 쿼리, 예상 답변`이며 기준일시는 `2026-03-05 09:00 (KST)`로 고정했습니다.

## Session Update (2026-04-23, HQ-as-owner dedup rewrite)

- `docs/golden-queries-hq-as-owner.csv` 200건을 기존 `golden-queries-store-owner.csv`와 의미 중복이 없도록 전면 재작성했습니다.
- 기간 표현 차이(오늘/어제/최근 등)를 동일 의도로 정규화해 교집합 0건을 검증한 후 반영했습니다.

## Session Update (2026-04-23, HQ query simplification)

- 본사 관점 질문 200건을 초기 시연용으로 짧고 쉬운 문장으로 단순화했습니다.
- 단순화 이후에도 기존 점주 골든쿼리와 의미 중복 0건을 유지했습니다.
## Session Update (2026-04-23, HQ query tone simplification)

- HQ 질문셋 200건을 현장 대화형 말투(예: "어때?", "뭐부터 보면 돼?")로 단순화했습니다.
- 기존 점주 골든쿼리와 의미 중복 0건 조건은 유지했습니다.
## Session Update (2026-04-23, HQ query concrete values)

- HQ 골든쿼리 CSV의 `실제 쿼리`에서 바인딩 변수(`:store_id`, `:date_from`, `:date_to`)를 예시 실값으로 치환했습니다.
- 적용값: `POC_010`, `20260201`, `20260305`.
## Session Update (2026-04-23, HQ query columns split)

- `golden-queries-hq-as-owner.csv`의 SQL 컬럼을 `일반화 쿼리`와 `예시 쿼리`로 분리했습니다.
- 일반화 쿼리는 파라미터형(`:store_id/:date_from/:date_to`), 예시 쿼리는 실값형(`POC_010/20260201/20260305`)으로 정리했습니다.

## Session Update (2026-04-23, HQ golden query dataset 500건 + JSON 변환)

- `docs/golden-queries-hq-as-owner.csv`를 200건 → 500건으로 확장했습니다.
  - 186~198번 가용여부 ✅ 이나 UNAVAILABLE로 잘못 표기된 13건을 실 SQL로 수정했습니다.
  - 201~500번 신규 질문 300건을 3개 에이전트(채널매출/생산재고/발주) 및 6개 본사 직무 분포 기준으로 추가했습니다.
- 500건 전체 예상 답변을 질문 의도별 고유 가이드 문장으로 전면 재작성했습니다 (행별 중복 0건).
- `docs/golden-queries-hq-as-owner.json`을 신규 생성했습니다.
  - 500건, 7개 필드 구조: `질문번호`, `사용자 질문`, `참고자료`, `가이드`, `검증사항1`(데이터 정합성 근거), `검수사항2`(답변 작성 가이드), `답변`(빈값)

- 요건 기반 골든쿼리 신규셋 `docs/golden-queries-new-02.csv`를 추가했습니다.
  - 전 에이전트 공통조건 + 매출/생산/주문 필수 질문과 파생 질문을 함께 구성했습니다.
  - 각 행은 `일반화 쿼리`와 `예시 쿼리`를 분리하고, 예상 답변에 즉시 실행 액션/근거 요구를 반영했습니다.
- `docs/golden-queries-new-02.csv`를 파생 질문 포함 112건으로 확장했습니다.

## Session Update (2026-04-24, golden query trace passthrough)

- AI 응답의 골든쿼리 메타(`matched_query_id`, `match_score`)를 backend `agent_trace`로 전달하도록 정비했습니다.
  - 변경 파일: `app/services/ai_client.py`, `app/schemas/sales.py`
- 매출 질의 완료 감사 로그(`sales_query_completed`) metadata에 골든쿼리 매칭 결과를 함께 기록합니다.
  - `matched_query_id`, `match_score`
  - 변경 파일: `app/services/sales_service.py`
- API 계약은 기존을 유지하면서, 골든쿼리 추적 필드만 옵션으로 확장했습니다.

## Session Update (2026-04-24, floating-chat system instruction forwarding)

- backend `/api/sales/query`에서 AI 호출 시 공통 시스템 프롬프트를 결합해 전달하도록 업데이트했습니다.
  - 기본 공통 규칙 + 설정 화면(system instruction) 병합 전달
- 응답 모델에 `follow_up_questions`를 추가해 설명/출처/액션/후속질문 3개 구조를 유지합니다.
- AI 응답의 `follow_up_questions`를 answer와 top-level에 전달하고, 누락 시 기본 3개 후속질문을 생성합니다.

## Session Update (2026-04-24, golden query pattern-ready)

- 골든쿼리 사용 방식이 원문 일치가 아닌 패턴 매칭 중심으로 강화되었습니다(AI 서비스).
- backend API 계약 변경은 없고, 기존 `/api/sales/query` 경로에서 동일하게 사용됩니다.

## Session Update (2026-04-25, settings logo alignment 영향도)

- 프론트 `/settings` 로고 정렬 작업(점주 유입 헤더와 동일 자산 적용)이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, production table JSX tag fix 영향도)

- 프론트 `ProductionTableSection` JSX 태그 정합성 수정이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, dashboard alert summary prop type fix 영향도)

- 프론트 `DashboardScreen` prop 타입 정리 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, settings logo click navigation 영향도)

- 프론트 `/settings` 로고 클릭 이동(`/`) 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, settings typography size alignment 영향도)

- 프론트 `/settings` 타이포그래피/헤더 사이즈 정렬 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, settings sidebar design-system alignment 영향도)

- 프론트 `/settings` 사이드바 디자인 시스템 정렬 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, settings sidebar rollback 영향도)

- 프론트 `/settings` 사이드바 스타일 롤백이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, settings summary cards white background 영향도)

- 프론트 `/settings` 요약 카드 배경 색상 통일(흰색) 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, prompts textarea width adjustment 영향도)

- 프론트 `/settings/prompts` textarea 폭 조정 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, prompts card equal height 영향도)

- 프론트 `/settings/prompts` 카드 높이 정렬 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, prompts card height 80 영향도)

- 프론트 `/settings/prompts` 카드/입력창 높이 통일 작업이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, floating chat golden-query integration 영향도)

- 프론트 플로팅 챗이 `/api/sales/query` 단일 경로 기반으로 골든쿼리 메타(`overlap_candidates`, `follow_up_questions`)를 우선 활용하도록 변경되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, floating chat source badge + reference popup 영향도)

- 프론트 플로팅 챗에 출처 배지/근거 팝업 UI가 추가되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, golden query miss badge 영향도)

- 프론트 플로팅 챗에 골든쿼리 미매칭 상태 배지 UI가 추가되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, dashboard recommended question handoff 영향도)

- 프론트 `/dashboard` 추천 질문 클릭 동작이 플로팅 챗 자동 질의로 변경되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, market page sales-trend card removal 영향도)

- 프론트 `/analytics/market` 카드 노출 조정이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, sales metrics info-popover coverage 영향도)

- 프론트 `/sales/metrics` 카드 설명 팝업(UI) 보강이 반영되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, floating chat suggested questions pinned to golden prompts 영향도)

- 프론트 플로팅 챗 후보 질문 소스가 골든 프롬프트 중심으로 조정되었습니다.
- 백엔드 API/스키마/마이그레이션 변경은 없습니다.

## Session Update (2026-04-25, ordering history chart date-axis alignment 영향도)

- 프론트 `/ordering/history` 차트 날짜 축 보정(필터 기간 전체 일자 표시)이 반영되었습니다.
- 백엔드 API/스키마/쿼리 변경은 없습니다.

## Session Update (2026-04-25, SPECIAL ordering basis removal 영향도)

- AI 주문 추천 계약에서 `SPECIAL(특별 기간)` 옵션 타입이 제거되었습니다.
- 백엔드 API/스키마/쿼리 변경은 없습니다.

## Session Update (2026-04-25, reference datetime default 09:00 영향도)

- 프론트 기준일시 기본값이 `2026-03-05T09:00`으로 조정되었습니다.
- backend docker-compose의 프론트 환경변수(`VITE_DEFAULT_REFERENCE_DATETIME`)도 `2026-03-05T09:00`으로 동기화했습니다.

## Session Update (2026-04-25, weekly revenue x-axis date+weekday tilt 영향도)

- 프론트 `/sales/metrics` 차트 X축 라벨 표기/기울기 UI 개선이 반영되었습니다.
- 백엔드 API/스키마/쿼리 변경은 없습니다.

## Session Update (2026-04-25, treemap chart height reduction 영향도)

- 프론트 `/sales/metrics` Treemap 높이 축소(UI) 변경이 반영되었습니다.
- 백엔드 API/스키마/쿼리 변경은 없습니다.

## Session Update (2026-04-25, analytics KPI curation for /analytics)

- `/api/analytics/metrics` 하단 KPI 구성을 재정의했습니다.
- 제외: `앱 주문 비중`, `할인 결제 비중`, 하단 카드의 `선택 기간 총 매출`
- 추가/대체: `투고 매출액`, `배달 매출액`, `런치 판매단가(~15시)`, `스윙타임 판매단가(15~17시)`, `디너 판매단가(17시~)`
- 유지: `홀 방문 고객`, `커피 동반 구매율`, 상단 `선택 기간 총 매출` 값(`selected_period_total_sales`) 제공
- 변경 파일:
  - `app/repositories/analytics_repository.py`
  - `tests/test_health.py`
- 검증:
  - `python -m py_compile app/repositories/analytics_repository.py tests/test_health.py` 통과
  - `pytest`는 로컬 DB(5435) 접속 제한으로 실행 실패

## Session Update (2026-04-25, takeout/delivery share of total sales)

- `/api/analytics/metrics`에서 `투고 매출액`, `배달 매출액` 카드 detail에 `전체 매출액 중 비중`을 추가했습니다.
- `selected_period_total_sales`를 분모로 사용해 비중을 계산합니다.
- 변경 파일:
  - `app/repositories/analytics_repository.py`
  - `tests/test_health.py`
- 검증: `python -m py_compile app/repositories/analytics_repository.py tests/test_health.py` 통과
