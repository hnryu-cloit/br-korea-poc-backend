# br-korea-poc-backend

BR Korea 매장 운영 지원 POC의 백엔드 API 서버입니다. FastAPI 기반의 REST API, PostgreSQL 데이터 적재 파이프라인, 감사 로그·운영 이력 관리 기능을 포함합니다. 현재 인터페이스 기준은 `br-korea-poc-front`입니다.

## 최근 업데이트 (2026-04-23)

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
│   │       │   ├── home.py             # GET /api/home/overview
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
| `GET /api/home/overview` | 홈 대시보드 요약 |
| `GET /api/data/catalog` | raw/core 테이블 목록 |
| `GET /api/data/preview/{table_name}` | 테이블 미리보기 |
| `GET /api/analytics/metrics` | 상단 운영 지표 |
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
- `notifications`, `home/overview`, `production/waste-summary`, `production/inventory-status` 500 원인을 정비해 `http://localhost:6003` Origin 기준 CORS 정상 응답(ACAO 포함)을 확인했습니다.
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
- 프론트 기본 환경값은 `VITE_DEFAULT_STORE_ID=POC_010`, `VITE_DEFAULT_REFERENCE_DATETIME=2026-03-05T00:00` 기준으로 동기화합니다.
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
