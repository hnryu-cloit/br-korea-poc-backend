# br-korea-poc-backend

BR Korea 매장 운영 지원 POC의 백엔드 API 서버입니다. 현재 코드는 운영 화면에서 사용하는 REST API, 데이터 적재 스크립트, 감사 로그/운영 이력 조회 기능을 포함합니다.

## Tech Stack

- FastAPI
- Pydantic v2 / pydantic-settings
- PostgreSQL
- SQLAlchemy
- psycopg2-binary
- httpx

## 현재 구현 범위

- 헬스체크 및 부트스트랩 API
- 데이터 카탈로그 / 테이블 미리보기 API
- 주문 옵션 / 선택 저장 / 이력 / 요약 API
- 생산 현황 / 알림 / 등록 / 이력 / 요약 API
- 매출 추천 질문 / 질의 / 구조화 인사이트 API
- 감사 로그 API
- 알림 인박스 API
- 매출 지표 API
- 본사 주문 코칭 / 생산 점검 API
- 매출 시그널 API

## Directory Structure

```text
app/                               # 백엔드 애플리케이션 루트
├── api/                           # FastAPI 라우터 계층
│   └── v1/
│       ├── endpoints/             # 도메인별 엔드포인트
│       │   ├── health.py          # 헬스체크
│       │   ├── bootstrap.py       # 초기 bootstrap 데이터
│       │   ├── data_catalog.py    # 데이터 카탈로그/미리보기
│       │   ├── ordering.py        # 주문 옵션/선택/이력
│       │   ├── production.py      # 생산 현황/등록/이력
│       │   ├── sales.py           # 매출 프롬프트/질의/인사이트
│       │   ├── audit.py           # 감사 로그
│       │   ├── notifications.py   # 알림 인박스
│       │   ├── analytics.py       # 상단 지표
│       │   ├── hq.py              # 본사 코칭/점검
│       │   ├── signals.py         # 매출 시그널
│       │   ├── channels.py        # 채널 초안
│       │   ├── review.py          # 리뷰 체크리스트
│       │   └── simulation.py      # 시뮬레이션 미리보기
│       └── router.py              # /api 라우터 묶음
├── core/                          # 설정, 인증, 의존성 보조 로직
│   ├── auth.py                    # 역할 식별 및 접근 제어
│   ├── config.py                  # 환경 설정
│   └── deps.py                    # 서비스 주입 함수
├── repositories/                  # DB 조회/저장 계층
│   ├── sales_repository.py
│   ├── ordering_repository.py
│   ├── production_repository.py
│   ├── audit_repository.py
│   └── ...                        # analytics/bootstrap/signals 등
├── schemas/                       # API/DB Pydantic 스키마
│   ├── sales.py
│   ├── ordering.py
│   ├── production.py
│   ├── audit.py
│   ├── data_catalog.py
│   ├── db_schemas.py              # 원본 DB 테이블 스키마 모델
│   └── ...
├── services/                      # 도메인 서비스 계층
│   ├── sales_service.py           # 매출 질의/인사이트
│   ├── ordering_service.py        # 주문 추천/선택 저장
│   ├── production_service.py      # 생산 현황/등록
│   ├── audit_service.py           # 감사 로그 처리
│   ├── analytics_service.py       # 지표 계산
│   ├── data_catalog_service.py    # 데이터 카탈로그 조회
│   └── ...
└── main.py                        # FastAPI 앱 엔트리

db/                                # DB 관련 파일
├── manifests/
│   └── resource_load_manifest.json# 원본 파일-raw 테이블 매핑
└── migrations/                    # SQL 마이그레이션
    ├── 0001_create_raw_resource_tables.sql
    ├── 0002_create_core_views.sql
    ├── 0003_create_operational_tables.sql
    └── 0004_add_store_id_to_operational_tables.sql

scripts/                           # 운영 스크립트
├── migrate_db.py                  # 마이그레이션 실행
├── load_resource_to_db.py         # resource 데이터 적재
└── inspect_resource_db.py         # 적재 상태 확인

tests/                             # 기본 API/연동 테스트
├── test_health.py
└── test_ai_client_integration.py
```

## 실행

```bash
open -a Docker
docker compose up -d postgres
python3 -m pip install -r requirements.txt
python scripts/migrate_db.py
python scripts/load_resource_to_db.py
uvicorn app.main:app --reload
```

- 기본 개발용 PostgreSQL 포트는 `5435`입니다.
- Swagger UI는 `/docs`, Redoc은 `/redoc`에서 확인할 수 있습니다.

## 데이터 적재

```bash
python scripts/inspect_resource_db.py
```

- 고객사 원본 데이터는 상위 `resource/`에 유지합니다.
- 적재 대상과 파일 매핑은 `db/manifests/resource_load_manifest.json`에서 관리합니다.
- 적재 데이터는 `raw_*` 테이블에 저장되고, 앱 조회는 `core_*` 뷰와 운영 테이블을 함께 사용합니다.
- 조회 확인용 API:
  - `/api/data/catalog`
  - `/api/data/preview/{table_name}`

## 권한 관련 메모

- 역할 식별은 `X-User-Role` 헤더를 사용합니다.
- 일부 엔드포인트는 역할 제한이 있으며, 예를 들어 감사 로그 조회는 허용 역할에서만 접근할 수 있습니다.
