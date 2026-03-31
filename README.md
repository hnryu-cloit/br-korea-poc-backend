# br-korea-poc-backend

BR Korea 매장 운영 지원 AI 에이전트 프로젝트의 **데이터 및 비즈니스 로직 API 서버**입니다.

## 🛠️ Tech Stack
-   **Framework**: FastAPI
-   **Database**: PostgreSQL (SQLAlchemy ORM)
-   **Dependency**: Pydantic v2
-   **Integration**: AI Service 연동 (REST API)

## 🏗️ Architecture
-   **Domain-Driven Design (DDD)** 패턴 적용: 
    -   `app/api/`: 엔드포인트 라우팅
    -   `app/services/`: 도메인 비즈니스 로직 및 AI 연동
    -   `app/repositories/`: DB 데이터 접근 제어
    -   `app/models/`: DB 스키마 정의

## 🚀 Key Integration
-   **Production Sync**: AI 서비스로부터 예측 데이터를 수신하여 화면에 표시.
-   **Ordering Logic**: AI 추천 옵션 기반의 주문 마감 알림 및 승인 프로세스.
-   **Sales Data Serving**: 매출 및 판매 데이터를 AI 분석용으로 가공하여 제공.

## 🏁 Run
```bash
open -a Docker
docker compose up -d postgres
python3 -m pip install -r requirements.txt
python scripts/migrate_db.py
python scripts/load_resource_to_db.py
uvicorn app.main:app --reload
```

- 기본 개발용 PostgreSQL 포트는 `5435`입니다.

## 📦 Data Load
```bash
python scripts/inspect_resource_db.py
```

- 고객사 원본은 상위 `resource/`에 유지합니다.
- DB 적재 대상과 파일 매핑은 `db/manifests/resource_load_manifest.json`으로 관리합니다.
- 적재 데이터는 PostgreSQL `raw_*` 테이블에 저장되고, 앱 조회는 `core_*` 뷰를 우선 사용합니다.
- API 조회는 `/api/data/catalog`, `/api/data/preview/{table_name}`로 확인할 수 있습니다.
