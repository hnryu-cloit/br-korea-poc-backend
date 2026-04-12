# DB 테이블 스키마 정의

> 출처: `POC 대상 테이블 구조(작성중)_V3.xlsx`
> 구현: FastAPI + Pydantic (`app/schemas/db_schemas.py`)

---

## 문서 목적

이 문서는 두 가지 목적을 함께 가집니다.

1. 고객사 원본 기준 테이블 스키마를 설명한다.
2. 현재 백엔드 코드 기준으로 원본 데이터가 어떤 적재/정제/운영 구조로 사용되는지 설명한다.

즉, 아래의 업무 테이블 설명은 원본 제공 데이터 기준이고, 실제 앱은 이 원본을 그대로 직접 조회하기보다 `raw_*` 적재 테이블, `core_*` 정제 뷰, 운영 테이블을 함께 사용한다.

## 현재 구현 기준 데이터 계층

현재 백엔드 DB 구조는 아래 3개 계층으로 나뉜다.

### 1. Raw 적재 테이블

- 목적: 원본 CSV/XLSX 데이터를 손실 없이 적재
- 생성 위치:
  - `db/migrations/0001_create_raw_resource_tables.sql`
  - `db/migrations/0005_create_new_workbook_raw_tables.sql`
  - `db/migrations/0006_create_campaign_raw_tables.sql`
  - `db/migrations/0007_create_settlement_and_telecom_raw_tables.sql`
- 예시:
  - `raw_store_master`
  - `raw_pay_cd`
  - `raw_daily_store_item_tmzon`
  - `raw_daily_store_cpi_tmzon`
  - `raw_daily_store_pay_way`
  - `raw_daily_store_item`
  - `raw_daily_store_online`
  - `raw_production_extract`
  - `raw_order_extract`
  - `raw_inventory_extract`
  - `raw_campaign_master`
  - `raw_campaign_item_group`
  - `raw_campaign_item`
  - `raw_settlement_master`
  - `raw_telecom_discount_type`
  - `raw_telecom_discount_policy`
  - `raw_telecom_discount_item`
  - `raw_workbook_rows`

특징:
- 대부분 컬럼이 `TEXT`로 적재된다.
- 원본 파일명, 시트명, 적재 시각(`source_file`, `source_sheet`, `loaded_at`)을 함께 저장한다.
- ingestion 이력은 `ingestion_runs`, `ingestion_files`에 기록된다.
- 생산/주문/재고/캠페인/정산/통신사 workbook의 주요 데이터 시트는 정식 raw 테이블로 직접 적재한다.
- 아직 정식 스키마가 없는 workbook은 우선 `raw_workbook_rows`에 보존 적재한다.

### 2. Core 정제 뷰

- 목적: 앱 조회용으로 타입을 정제하고 공통 형식을 맞춤
- 생성 위치: `db/migrations/0002_create_core_views.sql`
- 현재 구현된 뷰:
  - `core_store_master`
  - `core_daily_item_sales`
  - `core_hourly_item_sales`
  - `core_channel_sales`

특징:
- 공백 문자열을 `NULL`로 정리한다.
- 수치형 문자열을 `numeric`으로 캐스팅한다.
- 시간대 값은 `LPAD(..., 2, '0')`로 정규화한다.
- 앱의 분석/조회 서비스는 raw 테이블보다 core 뷰 사용을 우선한다.

### 3. 운영 테이블

- 목적: 사용자 행동, 감사 로그, 운영 등록 이력 저장
- 생성 위치:
  - `db/migrations/0003_create_operational_tables.sql`
  - `db/migrations/0004_add_store_id_to_operational_tables.sql`
- 현재 구현된 운영 테이블:
  - `audit_logs`
  - `ordering_selections`
  - `production_registrations`

특징:
- 원본 제공 데이터가 아니라 앱 사용 중 생성되는 데이터다.
- 프론트의 주문/생산 화면, 시스템 현황 화면이 직접 참조한다.

## 원본 데이터 적재 흐름

현재 적재 흐름은 다음과 같다.

1. 상위 `resource/` 폴더의 CSV/XLSX 원본을 읽는다.
2. `db/manifests/resource_load_manifest.json`에서 파일 경로와 대상 raw 테이블을 매핑한다.
3. `scripts/load_resource_to_db.py`가 해당 데이터를 `raw_*` 테이블에 적재한다.
4. `core_*` 뷰에서 타입 정제 후 앱 조회에 사용한다.

대표 매핑 예시는 아래와 같다.

| 원본 업무 테이블 | 현재 raw 테이블 | 현재 core 뷰 | 비고 |
|----------|--------|------|------|
| `STOR_MST` | `raw_store_master` | `core_store_master` | 점포 기본 정보 |
| `PAY_CD` | `raw_pay_cd` | - | 현재 코드상 별도 core 뷰 없음 |
| `DAILY_STOR_ITEM_TMZON` | `raw_daily_store_item_tmzon` | `core_hourly_item_sales` | 시간대/상품 매출 분석 기초 데이터 |
| `DAILY_STOR_CPI_TMZON` | `raw_daily_store_cpi_tmzon` | - | 현재 코드상 별도 core 뷰 없음 |
| `DAILY_STOR_PAY_WAY` | `raw_daily_store_pay_way` | - | 현재 코드상 별도 core 뷰 없음 |
| `DAILY_STOR_ITEM` | `raw_daily_store_item` | `core_daily_item_sales` | 상품/일자 매출 분석 기초 데이터 |
| `DAILY_STOR_ONLINE` | `raw_daily_store_online` | `core_channel_sales` | 채널/온오프라인 매출 분석 기초 데이터 |

신규 workbook 계열은 아래처럼 정식 raw 적재와 보존 적재가 나뉜다.

| 원본 workbook | 현재 적재 테이블 | 현재 앱 직접 사용 여부 | 비고 |
|----------|--------|------|------|
| `04. 생산/생산 데이터 추출.xlsx` | `raw_production_extract` | 아니오 | 생산 workbook 직접 적재 |
| `05. 주문/주문+데이터.xlsx` | `raw_order_extract` | 아니오 | 주문 workbook 직접 적재 |
| `06. 재고/재고+데이터+추출.xlsx` | `raw_inventory_extract` | 아니오 | 재고 workbook 직접 적재 |
| `07. 정산 기준 정보/*.xlsx` | `raw_settlement_master`, `raw_workbook_rows` | 아니오 | 정산 기준 direct load + 원본 보존 |
| `08. 통신사 제휴 할인 마스터/*.xlsx` | `raw_telecom_discount_type`, `raw_telecom_discount_policy`, `raw_telecom_discount_item`, `raw_workbook_rows` | 아니오 | 데이터 시트 direct load + 메타 시트 보존 |
| `09. 캠페인 마스터/캠페인+마스터.xlsx` | `raw_campaign_master`, `raw_campaign_item_group`, `raw_campaign_item` | 아니오 | 캠페인 workbook 3개 raw 테이블로 직접 적재 |

resource 기준으로 보면 현재 매핑은 아래처럼 해석하면 된다.

| resource 기준 파일/폴더 | manifest dataset | 생성/사용 raw 테이블 | 비고 |
|------|------|------|------|
| `03. 결제 수단 코드/결제 할인 수단 코드 테이블.csv` | `pay_code_csv` | `raw_pay_cd` | 결제/할인 코드 마스터 |
| `01. 점포 마스터/던킨+점포마스터_매핑용.xlsx` | `store_master` | `raw_store_master` | 점포 기준 정보 |
| `02. 매출/01. 일자별 시간대별 상품별 매출 데이터/*.xlsx` | `daily_store_item_tmzon` | `raw_daily_store_item_tmzon` | 6개 파일 direct load |
| `02. 매출/02. 일자별 시간대별 캠페인 매출 데이터/*.xlsx` | `daily_store_cpi_tmzon` | `raw_daily_store_cpi_tmzon` | direct load |
| `02. 매출/03. 일자별 결제 수단별 매출 데이터/*.xlsx` | `daily_store_pay_way` | `raw_daily_store_pay_way` | direct load |
| `02. 매출/04. 일자별 상품별 매출/*.xlsx` | `daily_store_item` | `raw_daily_store_item` | direct load |
| `02. 매출/05. 일자별 온_오프라인 구분/*.xlsx` | `daily_store_online` | `raw_daily_store_online` | direct load |
| `04. 생산/생산 데이터 추출.xlsx` | `production_extract` | `raw_production_extract` | `Sheet1` direct load |
| `05. 주문/주문+데이터.xlsx` | `order_extract`, `order_workbook_rows` | `raw_order_extract`, `raw_workbook_rows` | 정식 적재 + workbook 보존 |
| `06. 재고/재고+데이터+추출.xlsx` | `inventory_extract` | `raw_inventory_extract` | `Sheet1` direct load |
| `07. 정산 기준 정보/*.xlsx` | `settlement_master_extract`, `settlement_master_workbook` | `raw_settlement_master`, `raw_workbook_rows` | 정식 적재 + workbook 보존 |
| `08. 통신사 제휴 할인 마스터/*.xlsx` | `telecom_discount_type`, `telecom_discount_policy`, `telecom_discount_item`, `telecom_discount_master_workbook` | `raw_telecom_discount_type`, `raw_telecom_discount_policy`, `raw_telecom_discount_item`, `raw_workbook_rows` | 시트별 direct load + workbook 보존 |
| `09. 캠페인 마스터/캠페인+마스터.xlsx` | `campaign_master`, `campaign_item_group`, `campaign_item` | `raw_campaign_master`, `raw_campaign_item_group`, `raw_campaign_item` | 시트별 direct load |
| `00. 테이블 구조/*.xlsx`, `ERD_V0.2.png`, `POC_TABLE_DDL.sql` | - | - | 참고용 파일, 적재 대상 아님 |

## 현재 앱 사용 방식

현재 코드 기준으로 데이터는 아래처럼 사용된다.

### 매출/분석 계열

- 점포/기본 정보: `core_store_master`
- 일자별 상품 매출: `core_daily_item_sales`
- 시간대별 상품 매출: `core_hourly_item_sales`
- 채널별 매출: `core_channel_sales`

이 계층은 아래 기능의 기초 데이터로 사용된다.

- 매출 질의 API
- 매출 구조화 인사이트 API
- 데이터 카탈로그 / 미리보기 API

### 운영 저장 계열

- 감사/오케스트레이션 추적: `audit_logs`
- 주문 선택 저장 및 이력: `ordering_selections`
- 생산 등록 저장 및 이력: `production_registrations`

이 계층은 아래 기능과 직접 연결된다.

- 주문 관리 화면
- 생산 현황 화면
- 시스템 현황 / 매출 조회 화면의 로그 표시

### 신규 workbook raw 계열

- 생산 workbook은 `raw_production_extract`로 직접 적재된다.
- 주문 workbook은 `raw_order_extract`로 직접 적재하고, `Sheet2`는 `raw_workbook_rows`에 보존한다.
- 재고 workbook은 `raw_inventory_extract`로 직접 적재된다.
- 캠페인 workbook은 `raw_campaign_master`, `raw_campaign_item_group`, `raw_campaign_item`으로 직접 적재된다.
- 정산 기준 정보 workbook은 `raw_settlement_master`로 직접 적재되고, 원본 workbook도 `raw_workbook_rows`에 보존된다.
- 통신사 제휴 할인 마스터 workbook은 데이터 시트 3개가 `raw_telecom_discount_type`, `raw_telecom_discount_policy`, `raw_telecom_discount_item`으로 직접 적재되고, 메타 시트 보존을 위해 workbook 전체가 `raw_workbook_rows`에도 남는다.
- 현재 앱 서비스는 새 raw 테이블을 직접 조회하지 않으므로, 기능 확장은 이후 core 뷰/서비스 연결 단계에서 이루어진다.

## 현재 문서 해석 시 주의사항

- 아래 컬럼 정의는 원본 제공 문서 기준 업무 스키마 설명이다.
- 실제 DB 적재 시에는 raw 테이블 컬럼명이 일부 단순화되거나 원본과 다를 수 있다.
  - 예: 원본 `STOR_MST`는 현재 `raw_store_master`로 적재된다.
- 일부 원본 테이블은 현재 코드에서 적재만 되어 있고, 아직 별도 `core_*` 뷰나 서비스 로직으로 직접 연결되지 않았다.
- 반대로 `audit_logs`, `ordering_selections`, `production_registrations`는 원본 파일에 없는 현재 앱 운영용 테이블이다.
- `raw_workbook_rows`는 임시 스테이징 성격이 강하므로, 이 문서의 업무 스키마 표와 1:1로 대응하지 않을 수 있다.
- `raw_workbook_rows`는 원본 workbook 보존용이라서, 주문 workbook의 `Sheet2` 같은 메타 시트도 포함될 수 있다.

## 테이블 목록

| 테이블명 | 현재 raw 테이블 | 현재 core 뷰 | 설명 |
|----------|--------|------|------|
| `STOR_MST` | `raw_store_master` | `core_store_master` | 점포 기본 정보 |
| `PAY_CD` | `raw_pay_cd` | - | 결제/할인 코드 마스터 |
| `DAILY_STOR_ITEM_TMZON` | `raw_daily_store_item_tmzon` | `core_hourly_item_sales` | 시간대별 상품 매출 상세 |
| `DAILY_STOR_CPI_TMZON` | `raw_daily_store_cpi_tmzon` | - | 캠페인별 매출 집계 |
| `DAILY_STOR_PAY_WAY` | `raw_daily_store_pay_way` | - | 결제수단별 매출 집계 |
| `DAILY_STOR_ITEM` | `raw_daily_store_item` | `core_daily_item_sales` | 상품별 일자 매출 집계 |
| `DAILY_STOR_ONLINE` | `raw_daily_store_online` | `core_channel_sales` | 채널(온/오프라인)별 매출 |
| 주문 workbook 보조 시트 | `raw_workbook_rows` | - | 주문 workbook `Sheet2` 보존 적재 |
| 생산/주문/재고 workbook | `raw_production_extract`, `raw_order_extract`, `raw_inventory_extract` | - | 정식 raw 테이블로 직접 적재 |
| 캠페인 workbook | `raw_campaign_master`, `raw_campaign_item_group`, `raw_campaign_item` | - | 캠페인 3개 시트별 raw 테이블로 직접 적재 |
| 정산 기준 정보 workbook | `raw_settlement_master`, `raw_workbook_rows` | - | 정식 raw direct load + 원본 보존 |
| 통신사 제휴 할인 마스터 workbook | `raw_telecom_discount_type`, `raw_telecom_discount_policy`, `raw_telecom_discount_item`, `raw_workbook_rows` | - | 데이터 시트 direct load + 메타 시트 보존 |

## 운영 테이블 목록

| 테이블명 | 용도 | 주요 사용 화면/API |
|----------|------|------|
| `audit_logs` | 질의 라우팅, 차단, 응답 처리 감사 로그 저장 | `/api/audit/logs`, 시스템 현황, 매출 조회 |
| `ordering_selections` | 점주의 주문 선택 저장 | `/api/ordering/selections`, 주문 관리 |
| `production_registrations` | 생산 등록 저장 | `/api/production/registrations`, 생산 현황 |

---

## STOR_MST — 점포 마스터

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| MASKED_STOR_CD | VARCHAR | str | 점포코드 |
| MASKED_STOR_NM | VARCHAR | str | 점포명 |

---

## PAY_CD — 결제수단 코드

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| PAY_WAY_CD | VARCHAR | str | 결제/할인 그룹코드 |
| PAY_WAY_NM | VARCHAR | str | 결제/할인 그룹명 |
| PAY_DTL_CD | VARCHAR | str | 결제/할인 코드 |
| PAY_DTL_NM | VARCHAR | str | 결제/할인 코드명 |
| PAY_DC_TYPE | VARCHAR | str | 결제/할인 구분 |
| PAY_DC_TYPE_NM | VARCHAR | str | 결제/할인 구분명 |

---

## DAILY_STOR_ITEM_TMZON — 일자·시간대·상품별 매출

> 금액 계산 규칙
> - `ACTUAL_SALE_AMT` = `SALE_AMT` - `DC_AMT`
> - `NET_SALE_AMT` = `SALE_AMT` - `DC_AMT` - `VAT_AMT`
> - 부가세율: 10%

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| MASKED_STOR_CD | VARCHAR2(10) | str | 점포코드 |
| MASKED_STOR_NM | VARCHAR2(20) | str | 점포명 |
| ITEM_CD | VARCHAR2 | Optional[str] | 상품코드 |
| ITEM_NM | VARCHAR2(20) | str | 상품명 |
| SALE_DT | VARCHAR2(8) | str | 판매일자 (YYYYMMDD) |
| TMZON_DIV | VARCHAR2(20) | str | 시간대 구분 |
| SALE_QTY | NUMBER(10,0) | int | 판매수량 |
| SALE_AMT | NUMBER(10,0) | int | 판매금액 |
| RTN_QTY | NUMBER(10,0) | int | 반품수량 |
| RTN_AMT | NUMBER(10,0) | int | 반품금액 |
| DC_AMT | NUMBER(10,0) | int | 할인금액 |
| ENURI_AMT | NUMBER(10,0) | int | 에누리금액 |
| VAT_AMT | NUMBER(10,0) | int | 부가세금액 |
| ACTUAL_SALE_AMT | NUMBER(10,0) | int | 실매출금액 |
| NET_SALE_AMT | NUMBER(10,0) | int | 순매출금액 |
| TAKE_IN_AMT | NUMBER(10,0) | int | TAKE IN 금액 |
| TAKE_IN_VAT_AMT | NUMBER(10,0) | int | TAKE IN 부가세 |
| TAKE_OUT_AMT | NUMBER(10,0) | int | TAKE OUT 금액 |
| TAKE_OUT_VAT_AMT | NUMBER(10,0) | int | TAKE OUT 부가세 |
| SVC_FEE_AMT | NUMBER(10,0) | int | 봉사료 금액 |
| SVC_FEE_VAT_AMT | NUMBER(10,0) | int | 봉사료 부가세 |
| REG_USER_ID | VARCHAR2(50) | Optional[str] | 등록자 ID |
| REG_DATE | DATE | Optional[datetime] | 등록일시 |
| UPD_USER_ID | VARCHAR2(50) | Optional[str] | 수정자 ID |
| UPD_DATE | DATE | Optional[datetime] | 수정일시 |

---

## DAILY_STOR_CPI_TMZON — 일자별 시간대별 캠페인 매출

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| CMP_CD | VARCHAR2(4) | str | 회사코드 |
| SALE_DT | VARCHAR2(8) | str | 판매일자 (YYYYMMDD) |
| MASKED_STOR_CD | VARCHAR2(10) | str | 점포코드 |
| CPI_CD | VARCHAR2(14) | str | 캠페인코드 |
| CPI_ADD_ACCUM_POINT | NUMBER(15,2) | float | 캠페인 추가 적립 포인트 |
| CPI_CUST_USE_POINT | NUMBER(15,2) | float | 캠페인 고객 사용 포인트 |
| CPI_DC_QTY | NUMBER(10,0) | int | 캠페인 할인 수량 |
| CPI_DC_AMT | NUMBER(15,2) | float | 캠페인 할인 금액 |
| CPI_CUSTCNT | NUMBER(15,2) | float | 캠페인 고객수 |
| CPI_BILLCNT | NUMBER(15,2) | float | 캠페인 영수건수 |
| TOTSALE_QTY | NUMBER(10,0) | int | 총 판매수량 |
| TOTSALE_AMT | NUMBER(15,2) | float | 총 판매금액 |
| TOTDC_AMT | NUMBER(15,2) | float | 총 할인금액 |
| TOTACTUAL_SALE_AMT | NUMBER(15,2) | float | 총 실매출금액 |
| TOTVAT_AMT | NUMBER(15,2) | float | 총 부가세 |
| TOTNET_SALE_AMT | NUMBER(15,2) | float | 총 순매출금액 |
| TOTBILLCNT | NUMBER(15,2) | float | 총 영수건수 |
| TOTCUSTCNT | NUMBER(15,2) | float | 총 고객수 |
| REG_USER_ID | VARCHAR2(20) | Optional[str] | 등록자 ID |
| REG_DATE | DATE | Optional[datetime] | 등록일시 |
| UPD_USER_ID | VARCHAR2(20) | Optional[str] | 수정자 ID |
| UPD_DATE | DATE | Optional[datetime] | 수정일시 |

---

## DAILY_STOR_PAY_WAY — 일자별 결제수단별 매출

> PAY_WAY_CD 코드표
> `00`:현금 `01`:수표 `02`:신용카드 `03`:제휴할인(통신사) `04`:포인트사용
> `06`:상품권 `07`:알리페이 `08`:쿠폰 `09`:모바일CASH `10`:선불카드
> `11`:모바일CON `12`:직원결제 `13`:외상 `14`:외화 `15`:예약
> `16`:직원할인 `17`:임의할인 `99`:기타결제

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| CMP_CD | VARCHAR2(4) | str | 회사코드 |
| SALE_DT | VARCHAR2(8) | str | 판매일자 (YYYYMMDD) |
| MASKED_STOR_CD | VARCHAR2(10) | str | 점포코드 |
| PAY_WAY_CD | VARCHAR2(2) | str | 결제수단코드 |
| PAY_DTL_CD | VARCHAR2(2) | str | 결제 세부코드 |
| PAY_AMT | NUMBER(15,2) | float | 결제금액 |
| REC_AMT | NUMBER(15,2) | float | 받은금액 |
| CHANGE | NUMBER(15,2) | float | 거스름돈 |
| RTN_PAY_AMT | NUMBER(15,2) | float | 반품 결제금액 |
| RTN_REC_AMT | NUMBER(15,2) | float | 반품 받은금액 |
| RTN_CHANGE | NUMBER(15,2) | float | 반품 거스름돈 |
| ETC_PROFIT_AMT | NUMBER(15,2) | float | 기타 수익금액 |
| RTN_ETC_PROFIT_AMT | NUMBER(15,2) | float | 반품 기타 수익금액 |
| CASH_EXCHNG_CPN | NUMBER(15,2) | float | 현금 교환권 |
| RTN_CASH_EXCHNG_CPN | NUMBER(15,2) | float | 반품 현금 교환권 |
| REG_USER_ID | VARCHAR2(50) | Optional[str] | 등록자 ID |
| REG_DATE | DATE | Optional[datetime] | 등록일시 |
| UPD_USER_ID | VARCHAR2(50) | Optional[str] | 수정자 ID |
| UPD_DATE | DATE | Optional[datetime] | 수정일시 |

---

## DAILY_STOR_ITEM — 일자별 상품별 매출

> `ITEM_TAX_DIV`: 상품 과세 구분 (M0018 코드표 참조)

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| CMP_CD | VARCHAR2(4) | str | 회사코드 |
| SALE_DT | VARCHAR2(8) | str | 판매일자 (YYYYMMDD) |
| MASKED_STOR_CD | VARCHAR2(10) | str | 점포코드 |
| ITEM_CD | VARCHAR2(20) | str | 상품코드 |
| ITEM_TAX_DIV | VARCHAR2(1) | Optional[str] | 상품 과세 구분 |
| SALE_QTY | NUMBER(10,0) | int | 판매수량 |
| SALE_AMT | NUMBER(15,2) | float | 판매금액 |
| RTN_QTY | NUMBER(10,0) | int | 반품수량 |
| RTN_AMT | NUMBER(15,2) | float | 반품금액 |
| DC_AMT | NUMBER(15,2) | float | 할인금액 |
| ENURI_AMT | NUMBER(15,2) | float | 에누리금액 |
| VAT_AMT | NUMBER(15,2) | float | 부가세금액 |
| ACTUAL_SALE_AMT | NUMBER(15,2) | float | 실매출금액 |
| NET_SALE_AMT | NUMBER(15,2) | float | 순매출금액 |
| TAKE_IN_AMT | NUMBER(15,2) | float | TAKE IN 금액 |
| TAKE_IN_VAT_AMT | NUMBER(15,2) | float | TAKE IN 부가세 |
| TAKE_OUT_AMT | NUMBER(15,2) | float | TAKE OUT 금액 |
| TAKE_OUT_VAT_AMT | NUMBER(15,2) | float | TAKE OUT 부가세 |
| SVC_FEE_AMT | NUMBER(15,2) | float | 봉사료 금액 |
| SVC_FEE_VAT_AMT | NUMBER(15,2) | float | 봉사료 부가세 |
| REG_USER_ID | VARCHAR2(50) | Optional[str] | 등록자 ID |
| REG_DATE | DATE | Optional[datetime] | 등록일시 |
| UPD_USER_ID | VARCHAR2(50) | Optional[str] | 수정자 ID |
| UPD_DATE | DATE | Optional[datetime] | 수정일시 |

---

## DAILY_STOR_ONLINE — 일자별 온/오프라인 매출

> `HO_CHNL_DIV`: 판매유형 구분 (온라인 / 오프라인)

| 컬럼명 | 타입 | Python 타입 | 설명 |
|--------|------|-------------|------|
| MASKED_STOR_CD | VARCHAR | str | 점포코드 |
| MASKED_STOR_NM | VARCHAR | str | 점포명 |
| SALE_DT | VARCHAR | str | 판매일자 (YYYYMMDD) |
| TMZON_DIV | VARCHAR | str | 판매시간대 |
| HO_CHNL_CD | VARCHAR | str | 판매채널코드 |
| HO_CHNL_NM | VARCHAR | str | 판매채널명 |
| SALES_ORG_NM | VARCHAR | str | 영업조직 |
| HO_CHNL_DIV | VARCHAR | str | 판매유형 (온/오프라인) |
| SALE_AMT | NUMBER | float | 판매금액 |
| ORD_CNT | NUMBER | int | 판매수량 |

---

## 현재 API별 참조 테이블/뷰

아래는 현재 백엔드 코드 기준으로, 주요 API가 어떤 테이블 또는 뷰를 우선 참조하는지 정리한 목록이다.

### 데이터 조회/메타데이터 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/data/catalog` | 전체 테이블 메타데이터 | DB에 존재하는 테이블 목록과 row count를 동적으로 조회한다. |
| `GET /api/data/preview/{table_name}` | 요청한 테이블 전체 | 지정한 테이블을 그대로 preview 한다. raw/core/운영 테이블 모두 대상이 될 수 있다. |

### 주문 관리 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/ordering/options` | `core_daily_item_sales` 우선, 없으면 `raw_daily_store_item` | 최근 판매량 기준으로 주문 옵션의 상품/수량을 계산한다. |
| `POST /api/ordering/selections` | `ordering_selections` | 점주의 최종 주문 선택을 저장한다. |
| `GET /api/ordering/selections/history` | `ordering_selections` | 저장된 주문 선택 이력을 조회한다. |
| `GET /api/ordering/selections/summary` | `ordering_selections` | 최근 주문 선택 상태와 요약 지표를 계산한다. |
| `GET /api/ordering/context/{notification_id}` | DB 직접 참조 없음 | 현재는 정적 컨텍스트 응답이다. |
| `GET /api/ordering/alerts` | DB 직접 참조 없음 | 현재는 서비스 로직 중심 알림 응답이다. |

### 생산 관리 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/production/overview` | `core_hourly_item_sales` 우선, 없으면 `raw_daily_store_item_tmzon` | 최신 일자 판매 데이터를 기준으로 생산 대상 품목을 계산한다. |
| `GET /api/production/alerts` | `core_hourly_item_sales` / `raw_daily_store_item_tmzon` 기반 계산 | 생산 위험 SKU를 서비스 로직으로 도출한다. |
| `POST /api/production/registrations` | `production_registrations` | 생산 등록과 피드백 결과를 저장한다. |
| `GET /api/production/registrations/history` | `production_registrations` | 생산 등록 이력을 조회한다. |
| `GET /api/production/registrations/summary` | `production_registrations` | 최근 생산 등록 요약 지표를 계산한다. |

### 매출 분석 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/sales/prompts` | DB 직접 참조 없음 | 추천 질문 목록은 현재 코드상 정적 데이터다. |
| `POST /api/sales/query` | `core_channel_sales` 우선, 없으면 `raw_daily_store_online` | 배달/온라인 관련 질의는 채널 매출 데이터를 우선 사용한다. 그 외 질의는 스텁 응답 또는 서비스 로직을 사용한다. |
| `GET /api/sales/insights` | `core_hourly_item_sales`, `core_channel_sales`, `raw_daily_store_pay_way`, `core_daily_item_sales` | 피크타임, 채널 믹스, 결제 믹스, 메뉴 믹스 인사이트를 각각 다른 소스에서 계산한다. |

### 감사 로그 / 시스템 현황 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/audit/logs` | `audit_logs` | 질의 라우팅, 차단, 응답 처리 로그를 조회한다. |

### 알림 / 부트스트랩 / 리뷰 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/notifications` | DB 직접 참조 없음 또는 제한적 | 현재는 알림 서비스의 응답 중심이며, 운영 이벤트의 완전한 DB 연동 구조는 아님. |
| `GET /api/bootstrap` | DB 직접 참조 없음 | 제품/목표/정책/기능 설명용 bootstrap 데이터다. |
| `GET /api/channels/drafts` | DB 직접 참조 없음 | 채널별 초안 데이터는 현재 서비스 로직에서 제공한다. |
| `GET /api/review/checklist` | DB 직접 참조 없음 | 리뷰 체크리스트는 현재 서비스 로직에서 제공한다. |
| `POST /api/simulation/preview` | 운영 DB 직접 참조 없음 | 입력 payload 기준 계산형 응답이다. |

### 지표 / 본사 / 시그널 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/analytics/metrics` | `core_channel_sales`, `core_daily_item_sales`, `raw_daily_store_pay_way`, `raw_settlement_master`, `raw_telecom_discount_policy` | 최근 7일 기준 매출, 온라인 비중, 객단가, 커피 동반 구매율과 할인 결제 비중을 계산한다. |
| `GET /api/hq/coaching` | DB 직접 참조 없음 | 현재는 정적 응답 기반 본사 코칭 데이터다. |
| `GET /api/hq/inspection` | DB 직접 참조 없음 | 현재는 정적 응답 기반 생산 점검 데이터다. |
| `GET /api/signals` | `core_channel_sales`, `core_store_master`, `production_registrations`, `core_daily_item_sales`, `raw_settlement_master`, `raw_telecom_discount_policy`, `raw_telecom_discount_item`, `raw_daily_store_pay_way` | 지역별 배달 감소, 생산 대응 집중, 커피 동반 구매 강세, 활성 제휴 할인 운영 점검 시그널을 계산한다. |

## 해석 메모

- "우선 참조"는 코드상 `has_table(...)` 확인 뒤 가장 먼저 사용하는 소스를 의미한다.
- 동일 API라도 DB가 없거나 대상 테이블이 없으면 스텁 응답으로 폴백할 수 있다.
- 특히 `notifications`, `hq/coaching`, `hq/inspection`, `bootstrap`, `review`, `channels`는 현재 코드상 DB 직접 참조보다 서비스 내부 데이터 비중이 높다.

## 신규 workbook 후속 모델링 계획

현재는 생산/주문/재고/캠페인/정산/통신사 workbook의 주요 데이터 시트가 정식 raw 테이블로 분리되었다. 다만 주문 workbook의 보조 시트와 정산/통신사 workbook의 메타 보존은 여전히 `raw_workbook_rows`에 남아 있다. 앞으로는 이 raw 계층을 바탕으로 core 뷰와 서비스 연결을 단계적으로 확장하면 된다.

1. 생산/주문/재고/캠페인 raw 테이블의 컬럼 타입과 키를 업무 규칙에 맞춰 정제한다.
2. 필요 지표를 `core_*` 뷰 또는 mart 성격의 집계 뷰로 정제한다.
3. `production_service`, `ordering_service`, `sales_service`가 새 raw/core 계층을 우선 사용하도록 확장한다.
4. 정산 기준 정보와 통신사 제휴 할인 마스터는 이미 정식 raw 테이블로 승격되었고, 다음 단계는 `sales_service` 외 다른 도메인으로 소비 범위를 넓히는 것이다.
5. 그 후에야 재고 역산, 찬스로스, 시즌성 가중치, 예측 근거 설명 같은 요구사항을 데이터 기반으로 구현할 수 있다.

## 운영 메모

- `run_id = 11` cleanup 이후 `raw_store_master`, 주요 매출 raw 테이블, `raw_pay_cd`는 legacy csv/NFD source가 제거되었고 최신 manifest 기준 source만 남아 있다.
