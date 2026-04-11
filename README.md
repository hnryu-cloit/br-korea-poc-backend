# br-korea-poc-backend

BR Korea 매장 운영 지원 POC의 백엔드 API 서버입니다. FastAPI 기반의 REST API, PostgreSQL 데이터 적재 파이프라인, 감사 로그·운영 이력 관리 기능을 포함합니다.

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
│       └── 0004_add_store_id_to_operational_tables.sql
├── scripts/
│   ├── migrate_db.py                   # SQL 마이그레이션 실행
│   ├── load_resource_to_db.py          # resource 파일 → DB 적재
│   ├── inspect_resource_db.py          # 적재 상태 조회
│   └── test_ai_client_integration.py   # AI 클라이언트 통합 테스트
├── tests/
│   └── test_health.py
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
| `AI_SERVICE_URL` | (빈 값) | AI 서비스 URL (미설정 시 stub 사용) |
| `AI_SERVICE_TOKEN` | (빈 값) | AI 서비스 인증 토큰 |
| `CORS_ORIGINS` | `http://localhost:5173` | 허용 Origin (쉼표 구분) |
| `APP_ENV` | `local` | 실행 환경 |
| `APP_PORT` | `8000` | 개발 서버 포트 |

## 실행

```bash
# PostgreSQL 컨테이너 기동 (포트 5435)
open -a Docker
docker compose up -d postgres

# 의존성 설치
pip install -r requirements.txt

# DB 마이그레이션
python scripts/migrate_db.py

# resource 데이터 적재
python scripts/load_resource_to_db.py

# 개발 서버 실행
uvicorn app.main:app --reload
```

- Swagger UI: `http://localhost:8000/docs`
- Redoc: `http://localhost:8000/redoc`

### Docker 빌드 (단독 컨테이너)

```bash
docker build -t br-korea-poc-backend .
docker run -p 6002:6002 --env-file .env br-korea-poc-backend
```

## API 엔드포인트

| 경로 | 설명 |
|---|---|
| `GET /health` | 서버 헬스체크 |
| `GET /api/health` | API 라우터 헬스체크 |
| `GET /api/bootstrap` | 앱 초기화 데이터 |
| `GET /api/data/catalog` | raw/core 테이블 목록 |
| `GET /api/data/preview/{table_name}` | 테이블 미리보기 |
| `GET /api/analytics/metrics` | 상단 운영 지표 |
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
| `GET /api/hq/coaching` | 본사 주문 코칭 |
| `GET /api/hq/inspection` | 본사 생산 점검 |

## 데이터 적재 파이프라인

원본 파일은 상위 `resource/` 디렉터리에 유지하며, 적재 대상·매핑은 `db/manifests/resource_load_manifest.json`에서 관리합니다.

```bash
# 적재 실행
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

- **매출·분석**: `core_daily_item_sales`, `core_hourly_item_sales`, `core_channel_sales`, `core_store_master` 뷰에 연결됩니다.
- **주문·생산 이력**: `ordering_selections`, `production_registrations`, `audit_logs` 운영 테이블을 사용합니다.
- **신규 workbook 데이터** (생산·주문·재고·캠페인): `raw_workbook_rows` 적재는 완료됐으나 서비스 계층이 아직 직접 읽지 않습니다. 정식 raw 테이블 모델링 및 repository 연결 작업이 남아 있습니다.

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

`AI_SERVICE_URL`이 설정돼 있으면 `SalesService`가 `AIServiceClient`를 통해 AI 서비스로 질의를 프록시합니다. 미설정 시 repository stub 응답을 반환합니다.