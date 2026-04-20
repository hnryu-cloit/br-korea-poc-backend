# br-korea-poc-backend

BR Korea 매장 운영 지원 POC의 백엔드 API 서버입니다. FastAPI 기반의 REST API, PostgreSQL 데이터 적재 파이프라인, 감사 로그·운영 이력 관리 기능을 포함합니다. 현재 인터페이스 기준은 `br-korea-poc-front`입니다.

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
│   │   └── deps.py                     # FastAPI 의존성 주입 함수
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
| `EXTERNAL_API_KEY` | `stub-key` | 공공데이터포털 API 키 (소진공 SmallShop 실시간 조회용) |
| `AI_SERVICE_URL` | `http://localhost:6001` | AI 서비스 URL (미설정 시 repository/fallback 계산 사용) |
| `AI_SERVICE_TOKEN` | (빈 값) | AI 서비스 인증 토큰 |
| `CORS_ORIGINS` | `http://localhost:5173,http://localhost:6003` | 허용 Origin (쉼표 구분) |
| `APP_ENV` | `local` | 실행 환경 |
| `APP_PORT` | `8000` | 내부 기본 포트 |

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
- fallback 처리 경로명은 프론트 표시 규칙에 맞춰 `stub_repository`를 사용합니다.

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
