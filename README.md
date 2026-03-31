# br-korea-poc-backend

> **전역 시스템 제약조건 및 코드 컨벤션**
> 본 프로젝트는 엔터프라이즈 B2B SaaS 아키텍처를 지향하며, 공통 코드 컨벤션을 따릅니다.
> 상세 기준은 상위 디자인 시스템 및 저장소 전역 컨벤션 문서를 우선 확인하세요.
> 주요 백엔드 제약: **Layered Architecture, Schema Validation, Typed Services**

br-korea-poc의 bootstrap API와 정책/상태 전달을 담당하는 백엔드 MVP 초안입니다.

## Summary

- 담당 도메인: 5분 단위로 생산 대상 품목 재고를 조회하고 현재 재고량을 화면과 AP…, 최근 판매 추세와 4주 평균 생산 패턴을 결합해 1시간 후 예상 재고량…, 재고 소진 1시간 전에 생산 필요 시점을 자동 감지하고 PUSH 알림을…, 알림에는 현재고, 1시간 후 추정 재고, 4주 평균 1차 및 2차 생산…
- 주요 사용자: 매장 점주: 생산, 주문, 매출 판단을 수행하는 최종 의사결정자, 매장 운영 담당자: 생산 등록과 재고 상황을 확인하는 실무 사용자, 본사 운영/기획 담당자: 모델 정확도, 운영 효율, 보안 정책 준수 여부를 검토하는 검증 사용자
- 핵심 역할:
- 5분 단위로 생산 대상 품목 재고를 조회하고 현재 재고량을 화면과 API로 제공한다.
- 최근 판매 추세와 4주 평균 생산 패턴을 결합해 1시간 후 예상 재고량을 예측한다.
- 재고 소진 1시간 전에 생산 필요 시점을 자동 감지하고 PUSH 알림을 발송한다.
- 알림에는 현재고, 1시간 후 추정 재고, 4주 평균 1차 및 2차 생산 시간과 수량을 포함한다.

## Policy Notes

- 생산과 주문 추천은 의사결정 보조로 한정하고 최종 승인 주체는 점주로 유지한다.
- 매출, 손익, 점포 성과, 생산량, 원가 정보는 민감정보로 분류하고 퍼블릭 LLM 직접 전송을 제한한다.
- 역할 기반 접근 제어를 적용해 점주, 매장 운영 담당자, 본사 담당자의 조회 범위를 구분한다.
- 질의 라우팅, 프롬프트, 응답, 추천 사유, 사용자 선택 결과를 감사 가능한 로그로 남긴다.

## Stack

- Python 3.9+
- FastAPI
- Uvicorn
- Pydantic

## Structure

```text
app/
├── api/
├── core/
├── models/
├── repositories/
├── schemas/
├── services/
└── main.py
```

## Conventions

- Router, Service, Repository 책임을 분리합니다.
- 입력과 출력은 스키마로 검증합니다.
- 정책과 상태 전이는 서비스 계층에서 일관되게 처리합니다.
- bootstrap payload도 API 계약으로 노출합니다.

## POC Architecture Principles

- 현재 백엔드는 운영 시스템 연동 전 단계의 데모 API 레이어로 유지합니다.
- 샘플 데이터 소스는 우선 in-memory 또는 CSV/Excel 적재용 repository로 다루고, 이후 실제 운영 시스템 연동 시 repository 구현을 교체합니다.
- 도메인 API는 `production`, `ordering`, `sales` 단위로 분리하고, 공통 정책이나 조합 로직만 서비스 계층에서 담당합니다.
- 라우터는 요청/응답 계약과 HTTP 처리만 담당하고, 데이터 가공과 상태 판단은 서비스 계층으로 올립니다.
- POC 단계에서는 인증/인가를 강제하지 않지만, 향후 추가될 수 있도록 엔드포인트 계약과 계층 경계를 명확히 유지합니다.
- 운영 연동 전까지는 검증 가능한 API 계약을 우선하며, 과도한 인프라 복잡도보다 교체 가능한 구조를 우선합니다.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Test

```bash
pytest
```

## Environment

- `.env.example` 제공
- `APP_NAME`, `APP_ENV`, `DATABASE_URL`, `EXTERNAL_API_KEY` 분리

## Review Points

- API 계약과 서비스 책임이 README에 드러나는가
- 스키마, 서비스, 저장소 계층이 구조와 대응되는가
- 실행과 테스트 방법이 누락되지 않았는가
