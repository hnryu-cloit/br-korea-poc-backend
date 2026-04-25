# Backend 코딩 가이드

## 목적

이 문서는 `br-korea-poc-backend` 레포지토리의 파일 배치, 계층 구조, 네이밍, 코딩 규칙을 일관되게 유지하기 위한 기준이다.

목표는 다음과 같다.

* 계층 간 책임을 명확히 분리한다.
* 새 기능을 추가할 때 파일 위치 판단 기준을 통일한다.
* AI를 활용한 코드 생성 및 리팩토링 작업 시에도 동일한 구조 원칙을 유지한다.

---

## 디렉터리 구조

```text
app
├── api
│   └── v1
│       └── endpoints
│           ├── sales.py
│           ├── ordering.py
│           ├── production.py
│           └── ...
├── core
│   ├── auth.py
│   ├── config.py
│   ├── deps.py
│   └── utils.py
├── infrastructure
│   └── db
│       └── utils.py
├── models
├── repositories
│   ├── base_repository.py
│   ├── sales_repository.py
│   ├── sales
│   │   ├── campaign_repository.py
│   │   ├── insight_repository.py
│   │   └── prompt_repository.py
│   └── ...
├── schemas
│   ├── sales.py
│   ├── ordering.py
│   └── ...
└── services
    ├── sales_service.py
    ├── ordering_service.py
    └── ...
```

---

## 계층 구조와 책임

### 기본 흐름

```
endpoint → service → repository → schema
```

각 계층의 책임은 아래와 같다.

| 계층 | 책임 | 금지 사항 |
|---|---|---|
| `endpoints` | request 파싱, DI, HTTPException 반환 | 비즈니스 로직 직접 구현 |
| `services` | 비즈니스 로직 조합, 여러 repository 협력 | DB 쿼리 직접 실행 |
| `repositories` | DB 접근, 쿼리 실행, 결과 반환 | 비즈니스 규칙 판단 |
| `schemas` | 요청/응답 Pydantic 모델 정의 | 로직 포함 금지 |

### endpoint 작성 규칙

* endpoint는 `request` 파싱과 DI 연결만 담당한다.
* 비즈니스 로직은 반드시 service로 위임한다.
* 예외는 endpoint에서 `HTTPException`으로 감싸 반환한다.
* 동일한 예외 매핑이 반복되면 endpoint 파일 내 private helper 함수로 공통화한다.

```python
@router.post("/query", response_model=SalesQueryResponse)
async def query_sales(
    payload: SalesQueryRequest,
    role: str = Depends(get_current_role),
    service: SalesService = Depends(get_sales_service),
) -> SalesQueryResponse:
    return await service.query(payload, actor_role=role)
```

잘못된 예시:

```python
@router.post("/query")
async def query_sales(payload: SalesQueryRequest):
    # endpoint에서 직접 DB 쿼리 또는 복잡한 로직 실행 — 금지
    conn = get_db()
    rows = conn.execute("SELECT ...")
    return {"result": rows}
```

### service 작성 규칙

* 비즈니스 로직 조합과 여러 repository 간 협력을 담당한다.
* 직접 DB 쿼리를 실행하지 않는다.
* 의존성은 `__init__`에서 주입받는다.

### repository 작성 규칙

* 모든 DB 접근은 `repositories/`에서만 이루어진다.
* 쿼리는 SQLAlchemy ORM보다 `text()` + `mappings()` 방식을 선호한다.
* `has_table()`로 테이블 존재 여부를 확인하고, 없을 경우 stub 폴백을 유지한다.

```python
if not has_table(self.engine, "core_daily_item_sales"):
    return stub_result

with self.engine.connect() as conn:
    rows = conn.execute(text("SELECT ..."), params).mappings().all()
```

도메인이 복잡해지면 Mixin으로 분리하고 메인 repository 클래스에서 조합한다.

```python
class SalesRepository(PromptRepositoryMixin, InsightRepositoryMixin, CampaignRepositoryMixin):
    ...
```

---

## 네이밍 규칙

### 파일명

* 모든 파일명은 `snake_case.py`를 사용한다.
* 파일명은 도메인 단위로 맞춘다.

```text
endpoints/sales.py
services/sales_service.py
repositories/sales_repository.py
schemas/sales.py
```

### 스키마(Pydantic 모델) 네이밍

* 응답 모델: `XxxResponse`
* 요청 모델: `XxxRequest`

```python
class SalesQueryRequest(BaseModel): ...
class SalesQueryResponse(BaseModel): ...
class SalesSummaryResponse(BaseModel): ...
```

### 서비스 클래스

* `XxxService` 형태를 사용한다.

```python
class SalesService: ...
class OrderingService: ...
```

### Repository 클래스

* `XxxRepository` 형태를 사용한다.
* Mixin은 `XxxRepositoryMixin` 형태를 사용한다.

---

## 인증 규칙

* 인증은 `X-User-Role` 헤더 기반이다.
* 헤더 미전송 시 `store_owner`로 기본 처리한다.
* `get_current_role` 의존성을 통해 role을 주입받는다.

```python
role: str = Depends(get_current_role)
```

* 특정 역할만 허용하는 경우 `require_roles` 데코레이터를 사용한다.

```python
role: str = Depends(require_roles("hq_admin", "hq_operator"))
```

---

## 스키마 파일 배치 규칙

* 도메인별로 하나의 파일에 관련 스키마를 모은다.
* 요청/응답 모델은 같은 파일에 함께 둔다.

```text
schemas/sales.py       # SalesQueryRequest, SalesQueryResponse, SalesSummaryResponse ...
schemas/ordering.py    # OrderingRequest, OrderingResponse ...
schemas/production.py  # ProductionRequest, ProductionResponse ...
```

공통 스키마(여러 도메인에서 공유)는 별도 파일로 분리한다.

```text
schemas/contracts.py   # 공통 계약 타입
schemas/db_schemas.py  # DB 스키마 메타데이터
```

---

## 의존성 주입(DI)

* DI 팩토리 함수는 `app/core/deps.py`에서 관리한다.
* 각 service 인스턴스는 DI를 통해 endpoint에 주입한다.

```python
# core/deps.py
def get_sales_service() -> SalesService:
    return SalesService(repository=SalesRepository(engine=get_engine()))
```

```python
# endpoint
service: SalesService = Depends(get_sales_service)
```

---

## core 폴더 가이드

`app/core`는 앱 전반에 걸쳐 사용되는 인프라성 파일만 둔다.

| 파일 | 역할 |
|---|---|
| `auth.py` | 인증/role 검증 의존성 |
| `config.py` | 환경 변수 설정 (pydantic-settings) |
| `deps.py` | DI 팩토리 함수 모음 |
| `utils.py` | 도메인 무관 공통 유틸 |

feature 전용 유틸이나 비즈니스 규칙은 `core`에 두지 않는다.

---

## 로깅 규칙

* 모듈 최상단에서 `logging.getLogger(__name__)`을 사용한다.
* 별도의 커스텀 logger가 필요한 경우에만 이름을 명시한다.

```python
import logging
logger = logging.getLogger(__name__)
```

---

## 주석 규칙

* 주석은 최소화하고 코드 자체로 의미 전달을 우선한다.
* WHY가 비자명한 경우에만 주석을 작성한다.
* docstring은 주요 public 메서드 단위로만 작성한다.
* 한국어로 작성한다.

```python
async def get_summary(self, store_id: str | None = None) -> dict:
    """오늘 매출 요약 및 최근 7일 주간 데이터, 상품별 매출 순위를 실 DB에서 집계"""
```

단순 getter, 1~2줄 함수에는 docstring을 달지 않는다.

---

## 파일 배치 판단 기준

| 질문 | 배치 위치 |
|---|---|
| HTTP 요청/응답 처리 | `endpoints/` |
| 비즈니스 로직 조합 | `services/` |
| DB 쿼리 실행 | `repositories/` |
| 요청/응답 타입 정의 | `schemas/` |
| 인증, DI, 전역 설정 | `core/` |
| DB 연결, 테이블 유틸 | `infrastructure/` |

---

## 금지 사항

* endpoint에서 직접 DB 쿼리 실행 금지
* service에서 `text()` 쿼리 직접 실행 금지
* `schemas/`에 비즈니스 로직 포함 금지
* 도메인 전용 로직을 `core/`에 두는 것 금지

---

## 최종 정리

> endpoint는 얇게, 비즈니스 로직은 service로, DB 접근은 repository로, 타입은 schema로.

세부 기준:

* 계층 흐름: `endpoint → service → repository → schema`
* 파일명: `snake_case.py`
* 스키마: 요청은 `XxxRequest`, 응답은 `XxxResponse`
* 쿼리: `text()` + `mappings()` 우선
* DB 접근 전 `has_table()` 확인 후 stub 폴백 유지
* 인증: `X-User-Role` 헤더, 미전송 시 `store_owner`
* 로깅: `logging.getLogger(__name__)`
* 주석: 한국어, 최소화, WHY 중심
