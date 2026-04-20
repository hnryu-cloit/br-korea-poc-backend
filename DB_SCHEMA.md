# DB 테이블 스키마 정의

> 출처: `POC 대상 테이블 구조(작성중)_V3.xlsx`
> 구현: FastAPI + Pydantic (`app/schemas/db_schemas.py`)

---

## 원본 테이블 분류

### 생산

| 원본 테이블명 | Comment | DB 테이블 |
|---|---|---|
| `PROD_DTL` | 생산 테이블 | `raw_production_extract` |

### 주문

| 원본 테이블명 | Comment | DB 테이블 |
|---|---|---|
| `ORD_DTL` | 주문 테이블 | `raw_order_extract` |

### 재고

| 원본 테이블명 | Comment | DB 테이블 |
|---|---|---|
| `SPL_DAY_STOCK_DTL` | 재고 테이블 | `raw_inventory_extract` |

### 매출

| 원본 테이블명 | Comment | DB 테이블 |
|---|---|---|
| `DAILY_STOR_ITEM_TMZON` | 일자별 시간대별 상품별 매출 데이터 | `raw_daily_store_item_tmzon`, `core_hourly_item_sales` |
| `DAILY_STOR_CPI` | 일자별 시간대별 캠페인 매출 데이터 | `raw_daily_store_cpi_tmzon` |
| `DAILY_STOR_PAY_WAY` | 일자별 결제 수단별 매출 데이터 | `raw_daily_store_pay_way` |
| `DAILY_STOR_ITEM` | 일자별 상품별 매출 | `raw_daily_store_item`, `core_daily_item_sales` |
| `DAILY_STOR_CHL_TMZON` | 일자별 온/오프라인 매출 | `raw_daily_store_online`, `core_channel_sales` |

### 마스터

| 원본 테이블명 | Comment | DB 테이블 |
|---|---|---|
| `STOR_MST` | 점포 마스터 | `raw_store_master`, `core_store_master` |
| `PAY_CD` | 결제수단 코드 | `raw_pay_cd` |
| `MST_TERM_COOP_CMP_PAY_DC` | 마스터 기간 제휴사 결제 할인 | `raw_telecom_discount_policy` |
| `MST_TERM_COOP_CMP_DC_ITEM` | 마스터 기간 제휴사 할인 상품 | `raw_telecom_discount_item` |
| `CPI_MST` | 캠페인 마스터 | `raw_campaign_master` |
| `CPI_ITEM_GRP_MNG` | 캠페인 상품 그룹 관리 | `raw_campaign_item_group` |
| `CPI_ITEM_MNG` | 캠페인 상품 관리 | `raw_campaign_item` |

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
| `04. 생산/생산 데이터 추출.xlsx` | `raw_production_extract` | 예 (`production_repository`) | 생산 현황/SKU 목록 계산에 우선 참조 |
| `05. 주문/주문+데이터.xlsx` | `raw_order_extract` | 예 (`ordering_repository`) | 주문 옵션 계산에 우선 참조 |
| `06. 재고/재고+데이터+추출.xlsx` | `raw_inventory_extract` | 예 (`production_repository`) | 재고 추정에 우선 참조 |
| `07. 정산 기준 정보/*.xlsx` | `raw_settlement_master`, `raw_workbook_rows` | 예 (`sales_repository`, `signals_repository`) | 결제·할인 인사이트 및 시그널 컨텍스트에 사용 |
| `08. 통신사 제휴 할인 마스터/*.xlsx` | `raw_telecom_discount_type`, `raw_telecom_discount_policy`, `raw_telecom_discount_item`, `raw_workbook_rows` | 예 (`sales_repository`, `signals_repository`) | 활성 제휴 할인 맥락, 인사이트, 시그널에 사용 |
| `09. 캠페인 마스터/캠페인+마스터.xlsx` | `raw_campaign_master`, `raw_campaign_item_group`, `raw_campaign_item` | 예 (`sales_repository`) | 캠페인 시즌성 보정 인사이트에 사용 |

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

> DB 테이블: `raw_store_master`, `core_store_master`

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | 비고 |
|---|---|---|---|---|
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR | 점포코드 | |
| MASKED_STOR_NM | `maked_stor_nm` | VARCHAR | 점포명 | DB 적재 시 오타 (s 누락) — 원본은 MASKED |

> `raw_store_master` 추가 컬럼 (Excel 원본 확장 데이터):
> `actual_sales_amt`, `campaign_sales_ratio`, `store_type`, `business_type`, `sido`, `region`, `shipment_center`, `store_area_pyeong`

---

## PAY_CD — 결제수단 코드

> DB 테이블: `raw_pay_cd`
>
> ⚠️ CSV 적재 시 컬럼명이 원본과 다르게 변환됨

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 |
|---|---|---|---|
| PAY_WAY_CD | `pay_dc_grp_type` | VARCHAR | 결제/할인 그룹코드 |
| PAY_WAY_NM | `entry_nm_1` | VARCHAR | 결제/할인 그룹명 |
| PAY_DTL_CD | `pay_dc_cd` | VARCHAR | 결제/할인 코드 |
| PAY_DTL_NM | `pay_dc_nm` | VARCHAR | 결제/할인 코드명 |
| PAY_DC_TYPE | `pay_dc_type` | VARCHAR | 결제/할인 구분 |
| PAY_DC_TYPE_NM | `entry_nm_2` | VARCHAR | 결제/할인 구분명 |

---

## DAILY_STOR_ITEM_TMZON — 일자·시간대·상품별 매출

> DB 테이블: `raw_daily_store_item_tmzon` / 정제 뷰: `core_hourly_item_sales`
>
> 금액 계산 규칙
> - `ACTUAL_SALE_AMT` = `SALE_AMT` - `DC_AMT`
> - `NET_SALE_AMT` = `SALE_AMT` - `DC_AMT` - `VAT_AMT`
> - 부가세율: 10%

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | 비고 |
|---|---|---|---|---|
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR2(10) | 점포코드 | |
| MASKED_STOR_NM | `masked_stor_nm` | VARCHAR2(20) | 점포명 | |
| ITEM_NM | `item_nm` | VARCHAR2(20) | 상품명 | |
| SALE_DT | `sale_dt` | VARCHAR2(8) | 판매일자 (YYYYMMDD) | |
| TMZON_DIV | `tmzon_div` | VARCHAR2(20) | 시간대 구분 | |
| ITEM_CD | `item_cd` | VARCHAR2(20) | 상품코드 | |
| SALE_QTY | `sale_qty` | NUMBER(10,0) | 판매수량 | |
| SALE_AMT | `sale_amt` | NUMBER(10,0) | 판매금액 | |
| RTN_QTY | `rtn_qty` | NUMBER(10,0) | 반품수량 | |
| RTN_AMT | `rtn_amt` | NUMBER(10,0) | 반품금액 | |
| DC_AMT | `dc_amt` | NUMBER(10,0) | 할인금액 | |
| ENURI_AMT | `enuri_amt` | NUMBER(10,0) | 에누리금액 | |
| VAT_AMT | `vat_amt` | NUMBER(10,0) | 부가세금액 | |
| ACTUAL_SALE_AMT | `actual_sale_amt` | NUMBER(10,0) | 실매출금액 = SALE_AMT - DC_AMT | |
| NET_SALE_AMT | `net_sale_amt` | NUMBER(10,0) | 순매출금액 = SALE_AMT - DC_AMT - VAT_AMT | |
| TAKE_IN_AMT | `take_in_amt` | NUMBER(10,0) | TAKE IN 금액 | |
| TAKE_IN_VAT_AMT | `take_in_vat_amt` | NUMBER(10,0) | TAKE IN 부가세 금액 | |
| TAKE_OUT_VAT | `take_out_amt` | NUMBER(10,0) | TAKE OUT 금액 | 원본 컬럼명 오타 (VAT→AMT), DB는 take_out_amt |
| TAKE_OUT_VAT_AMT | `take_out_vat_amt` | NUMBER(10,0) | TAKE OUT 부가세 금액 | |
| SVC_FEE_AMT | `svc_fee_amt` | NUMBER(10,0) | 봉사료 금액 | |
| SVC_FEE_VAT_AMT | `svc_fee_vat_amt` | NUMBER(10,0) | 봉사료 부가세 금액 | |
| REG_USER_ID | `reg_user_id` | VARCHAR2(50) | 등록자 ID | |
| REG_DATE | `reg_date` | DATE | 등록 일시 | |
| UPD_USER_ID | `upd_user_id` | VARCHAR2(50) | 수정자 ID | |
| UPD_DATE | `upd_date` | DATE | 수정 일시 | |

---

## DAILY_STOR_CPI — 일자별 시간대별 캠페인 매출

> DB 테이블: `raw_daily_store_cpi_tmzon`
>
> ⚠️ **구조 불일치**: Excel 원본이 Oracle 스키마와 다른 pivot 구조로 제공됨
> - Oracle 원본: 캠페인×날짜 행 단위, 집계 컬럼 (`TOTSALE_AMT` 등)
> - 실제 DB: 캠페인×점포 행 단위, 시간대별 열 분리 (`qty_00~23`, `dc_amt_00~23`, `act_amt_00~23`)
> - **`SALE_DT` 미적재** — 날짜 필터링 불가
> - 미적재 컬럼: `CMP_CD`, `SALE_DT`, `CPI_ADD_ACCUM_POINT`, `CPI_CUST_USE_POINT`, `CPI_DC_QTY`, `CPI_CUSTCNT`, `TOTSALE_AMT`, `TOTVAT_AMT`, `TOTNET_SALE_AMT`, `TOTCUSTCNT`

**원본 스키마 (Oracle DAILY_STOR_CPI)**

| 원본 컬럼명 | 타입 | 설명 | DB 대응 |
|---|---|---|---|
| CMP_CD | VARCHAR2(4) | 회사 코드 | 미적재 |
| SALE_DT | VARCHAR2(8) | 판매 일자 | **미적재** |
| MASKED_STOR_CD | VARCHAR2(10) | 점포 코드 | `masked_stor_cd` |
| CPI_CD | VARCHAR2(14) | 캠페인 코드 | `cpi_cd` |
| CPI_ADD_ACCUM_POINT | NUMBER(15,2) | 캠페인 추가 적립 포인트 | 미적재 |
| CPI_CUST_USE_POINT | NUMBER(15,2) | 캠페인 고객 사용 포인트 | 미적재 |
| CPI_DC_QTY | NUMBER(10,0) | 캠페인 할인 수량 | 미적재 |
| CPI_DC_AMT | NUMBER(15,2) | 캠페인 할인 금액 | `dc_amt_00~23` (시간대별) |
| CPI_CUSTCNT | NUMBER(15,2) | 캠페인 고객수 | 미적재 |
| CPI_BILLCNT | NUMBER(15,2) | 캠페인 영수건수 | `bill_cnt` |
| TOTSALE_QTY | NUMBER(10,0) | 판매 수량 | `qty_00~23` 합산으로 계산 가능 |
| TOTSALE_AMT | NUMBER(15,2) | 판매 금액 | 미적재 |
| TOTDC_AMT | NUMBER(15,2) | 할인 금액 | `dc_amt_00~23` 합산으로 계산 가능 |
| TOTACTUAL_SALE_AMT | NUMBER(15,2) | 실 매출 금액 | `act_amt_00~23` 합산으로 계산 가능 |
| TOTVAT_AMT | NUMBER(15,2) | 부가세 금액 | 미적재 |
| TOTNET_SALE_AMT | NUMBER(15,2) | 순 매출 금액 | 미적재 |
| TOTBILLCNT | NUMBER(15,2) | 영수건수 | `bill_cnt` |
| TOTCUSTCNT | NUMBER(15,2) | 고객수 | 미적재 |
| REG_USER_ID | VARCHAR2(20) | 등록자 ID | 미적재 |
| REG_DATE | DATE | 등록 일시 | 미적재 |
| UPD_USER_ID | VARCHAR2(20) | 수정자 ID | 미적재 |
| UPD_DATE | DATE | 수정 일시 | 미적재 |

**실제 DB 컬럼 (`raw_daily_store_cpi_tmzon`)**

| DB 컬럼명 | 설명 |
|---|---|
| `masked_stor_cd` | 점포코드 |
| `masked_stor_nm` | 점포명 |
| `cpi_cd` | 캠페인 코드 |
| `cpi_nm` | 캠페인명 (Oracle 스키마 외 추가) |
| `bill_cnt` | 영수건수 합계 |
| `qty_00`~`qty_23` | 시간대별(0~23시) 판매수량 |
| `dc_amt_00`~`dc_amt_23` | 시간대별 할인금액 |
| `act_amt_00`~`act_amt_23` | 시간대별 실매출금액 |

---

## DAILY_STOR_PAY_WAY — 일자별 결제수단별 매출

> DB 테이블: `raw_daily_store_pay_way`

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | 비고 |
|---|---|---|---|---|
| CMP_CD | 미적재 | VARCHAR2(4) | 회사코드 | |
| SALE_DT | `sale_dt` | VARCHAR2(8) | 판매일자 (YYYYMMDD) | |
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR2(10) | 점포코드 | |
| — | `masked_stor_nm` | — | 점포명 | 원본 외 추가 |
| PAY_WAY_CD | `pay_way_cd` | VARCHAR2(2) | 결제수단코드 | 코드표 아래 참조 |
| — | `pay_way_cd_nm` | — | 결제수단명 | 원본 외 추가 |
| PAY_DTL_CD | `pay_dtl_cd` | VARCHAR2(2) | 결제 세부코드 (PAY_CD 참조) | |
| — | `pay_dtl_cd_nm` | — | 결제 세부코드명 | 원본 외 추가 |
| PAY_AMT | `pay_amt` | NUMBER(15,2) | 결제금액 | |
| REC_AMT | `rec_amt` | NUMBER(15,2) | 받은금액 | |
| CHANGE | `change_amt` | NUMBER(15,2) | 거스름돈 | 원본: CHANGE |
| RTN_PAY_AMT | `rtn_pay_amt` | NUMBER(15,2) | 반품 결제금액 | |
| RTN_REC_AMT | `rtn_rec_amt` | NUMBER(15,2) | 반품 받은금액 | |
| RTN_CHANGE | `rtn_change` | NUMBER(15,2) | 반품 거스름돈 | |
| ETC_PROFIT_AMT | `etc_profit_amt` | NUMBER(15,2) | 기타 수익금액 | |
| RTN_ETC_PROFIT_AMT | `rtn_etc_profit_amt` | NUMBER(15,2) | 반품 기타 수익금액 | |
| CASH_EXCHNG_CPN | `cash_exchng_cpn` | NUMBER(15,2) | 현금 교환권 | |
| RTN_CASH_EXCHNG_CPN | `rtn_cash_exchng_cpn` | NUMBER(15,2) | 반품 현금 교환권 | |
| REG_USER_ID | `reg_user_id` | VARCHAR2(50) | 등록자 ID | |
| REG_DATE | `reg_date` | DATE | 등록 일시 | |
| UPD_USER_ID | `upd_user_id` | VARCHAR2(50) | 수정자 ID | |
| UPD_DATE | `upd_date` | DATE | 수정 일시 | |

> **PAY_WAY_CD 코드표**
> `00`:현금 `01`:수표 `02`:신용카드 `03`:제휴할인(통신사) `04`:포인트사용
> `06`:상품권 `07`:알리페이 `08`:쿠폰 `09`:모바일CASH `10`:선불카드
> `11`:모바일CON `12`:직원결제 `13`:외상 `14`:외화 `15`:예약
> `16`:직원할인 `17`:임의할인 `99`:기타결제

---

## DAILY_STOR_ITEM — 일자별 상품별 매출

> DB 테이블: `raw_daily_store_item` / 정제 뷰: `core_daily_item_sales`
>
> `ITEM_TAX_DIV`: 상품 과세 구분 (M0018 코드표 참조)
>
> `core_daily_item_sales` 뷰는 `TAKE_IN_AMT`, `TAKE_OUT_AMT`, `SVC_FEE_AMT` 등 제외, 분석용 핵심 컬럼만 포함

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | core 뷰 포함 |
|---|---|---|---|---|
| CMP_CD | 미적재 | VARCHAR2(4) | 회사코드 | — |
| SALE_DT | `sale_dt` | VARCHAR2(8) | 판매일자 (YYYYMMDD) | ✓ |
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR2(10) | 점포코드 | ✓ |
| — | `masked_stor_nm` | — | 점포명 (원본 외 추가) | ✓ |
| ITEM_CD | `item_cd` | VARCHAR2(20) | 상품코드 | ✓ |
| — | `item_nm` | — | 상품명 (원본 외 추가) | ✓ |
| ITEM_TAX_DIV | `item_tax_div` | VARCHAR2(1) | 상품 과세구분 | ✓ |
| SALE_QTY | `sale_qty` | NUMBER(10,0) | 판매수량 | ✓ |
| SALE_AMT | `sale_amt` | NUMBER(15,2) | 판매금액 | ✓ |
| RTN_QTY | `rtn_qty` | NUMBER(10,0) | 반품수량 | ✓ |
| RTN_AMT | `rtn_amt` | NUMBER(15,2) | 반품금액 | ✓ |
| DC_AMT | `dc_amt` | NUMBER(15,2) | 할인금액 | ✓ |
| ENURI_AMT | `enuri_amt` | NUMBER(15,2) | 에누리금액 | ✓ |
| VAT_AMT | `vat_amt` | NUMBER(15,2) | 부가세금액 (세율 10%) | ✓ |
| ACTUAL_SALE_AMT | `actual_sale_amt` | NUMBER(15,2) | 실매출금액 = SALE_AMT - DC_AMT | ✓ |
| NET_SALE_AMT | `net_sale_amt` | NUMBER(15,2) | 순매출금액 = SALE_AMT - DC_AMT - VAT_AMT | ✓ |
| TAKE_IN_AMT | `take_in_amt` | NUMBER(15,2) | TAKE IN 금액 | — |
| TAKE_IN_VAT_AMT | `take_in_vat_amt` | NUMBER(15,2) | TAKE IN 부가세 금액 | — |
| TAKE_OUT_AMT | `take_out_amt` | NUMBER(15,2) | TAKE OUT 금액 | — |
| TAKE_OUT_VAT_AMT | `take_out_vat_amt` | NUMBER(15,2) | TAKE OUT 부가세 금액 | — |
| SVC_FEE_AMT | `svc_fee_amt` | NUMBER(15,2) | 봉사료 금액 | — |
| SVC_FEE_VAT_AMT | `svc_fee_vat_amt` | NUMBER(15,2) | 봉사료 부가세 금액 | — |
| REG_USER_ID | `reg_user_id` | VARCHAR2(50) | 등록자 ID | — |
| REG_DATE | `reg_date` | DATE | 등록 일시 | — |
| UPD_USER_ID | `upd_user_id` | VARCHAR2(50) | 수정자 ID | — |
| UPD_DATE | `upd_date` | DATE | 수정 일시 | — |

---

## DAILY_STOR_CHL_TMZON — 일자별 온/오프라인 매출

> DB 테이블: `raw_daily_store_online` / 정제 뷰: `core_channel_sales`
>
> `HO_CHNL_DIV`: 판매유형 구분 (온라인 / 오프라인)
>
> 원본↔DB 컬럼 완전 일치 ✓

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 |
|---|---|---|---|
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR | 점포코드 |
| MASKED_STOR_NM | `masked_stor_nm` | VARCHAR | 점포명 |
| SALE_DT | `sale_dt` | VARCHAR | 판매일자 (YYYYMMDD) |
| TMZON_DIV | `tmzon_div` | VARCHAR | 판매시간대 |
| HO_CHNL_CD | `ho_chnl_cd` | VARCHAR | 판매채널코드 |
| SALES_ORG_NM | `sales_org_nm` | VARCHAR | 영업조직 |
| HO_CHNL_DIV | `ho_chnl_div` | VARCHAR | 판매유형 (온라인/오프라인) |
| HO_CHNL_NM | `ho_chnl_nm` | VARCHAR | 판매채널명 |
| SALE_AMT | `sale_amt` | NUMBER | 판매금액 |
| ORD_CNT | `ord_cnt` | NUMBER | 판매수량 |

---

## PROD_DTL — 생산 테이블

> DB 테이블: `raw_production_extract`
>
> **생산 차수 규칙 (Info1)**
> - `PROD_DGRE` 필드는 차수 구분 불가 (단일 레코드에 1~3차 수량이 모두 기록됨)
> - 1차 생산 수량: `prod_qty`, 2차 생산 수량: `prod_qty_2`, 3차 생산 수량: `prod_qty_3`
> - **총 생산 수량 = `prod_qty + prod_qty_2 + prod_qty_3`**
>
> **생산 시점 규칙 (Info2)**
> - 1차 생산 시점: `reg_date`
> - 2차 생산 시점: `upd_date`
> - 3차까지 있을 경우 2차 생산 시점은 알 수 없음

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | 비고 |
|---|---|---|---|---|
| CMP_CD | `cmp_cd` | VARCHAR2(4) | 회사 코드 | |
| PROD_DT | `prod_dt` | VARCHAR2(8) | 생산 일자 (YYYYMMDD) | |
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR2(10) | 마스킹 점포 코드 | |
| — | `masked_stor_nm` | — | 마스킹 점포명 | 원본 외 추가 |
| PROD_DGRE | `prod_dgre` | VARCHAR2(2) | 생산 차수 (차수 구분 불가) | |
| ITEM_CD | `item_cd` | VARCHAR2(20) | 상품 코드 | |
| — | `item_nm` | — | 상품명 | 원본 외 추가 |
| PROD_QTY | `prod_qty` | NUMBER(7,0) | **1차 생산 수량** | 총 생산량 계산 시 반드시 포함 |
| SALE_PRC | `sale_prc` | NUMBER(15,2) | 판매 단가 | |
| ITEM_COST | `item_cost` | NUMBER(15,2) | 상품 원가 | |
| PROD_QTY_2 | `prod_qty_2` | NUMBER(7,0) | **2차 생산 수량** | 총 생산량 계산 시 반드시 포함 |
| PROD_QTY_3 | `prod_qty_3` | NUMBER(7,0) | **3차 생산 수량** | 총 생산량 계산 시 반드시 포함 |
| REPROD_QTY | `reprod_qty` | NUMBER(7,0) | 재생산 수량 | |
| REG_USER_ID | `reg_user_id` | VARCHAR2(50) | 등록자 ID (1차 생산 등록자) | |
| REG_DATE | `reg_date` | DATE | **1차 생산 등록 일시** | |
| UPD_USER_ID | `upd_user_id` | VARCHAR2(50) | 수정자 ID | |
| UPD_DATE | `upd_date` | DATE | **2차 생산 수정 일시** | |

---

## ORD_DTL — 주문 테이블

> DB 테이블: `raw_order_extract`
>
> **수량 환산 규칙 (Info1)**
> - `ord_noqqty`: 주문 입수량 — 단위별 수량 지정
> - 낱개 환산: `ord_qty × ord_noqqty = 낱개 수량`
>
> **주문 vs 확정 수량 (Info2, Info3)**
> - `ord_qty`: 점주가 실제 주문한 수량
> - `confrm_qty`: 실제 센터에서 출하된 수량
> - 센터 재고·CAPA에 따라 상이할 수 있음 (미출 = `ord_qty - confrm_qty`)
>
> **권고 주문 수량 (Info4)**
> - `ord_rec_qty`: 던킨 시스템이 자동 계산한 권고 주문 수량 (강제 아님)
>
> ⚠️ Oracle `STOR_CD` → DB `masked_stor_cd` (점포코드 마스킹), `CMP_CD`·`REG_USER_ID` 등 미적재

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | 비고 |
|---|---|---|---|---|
| CMP_CD | — | VARCHAR2(4) | 회사 코드 | 미적재 |
| DLV_DT | `dlv_dt` | VARCHAR2(8) | 배송 일자 (YYYYMMDD) | |
| STOR_CD | `masked_stor_cd` | VARCHAR2(10) | 마스킹 점포 코드 | Oracle은 STOR_CD, DB에서 마스킹됨 |
| — | `masked_stor_nm` | — | 마스킹 점포명 | 원본 외 추가 |
| ORD_GRP | `ord_grp` | VARCHAR2(4) | 주문 그룹 | |
| — | `ord_grp_nm` | — | 주문 그룹명 | 원본 외 추가 |
| ORD_DGRE | `ord_dgre` | VARCHAR2(2) | 주문 차수 | |
| — | `ord_dgre_nm` | — | 주문 차수명 | 원본 외 추가 |
| ORD_TYPE | `ord_type` | VARCHAR2(2) | 주문 유형 | |
| — | `ord_type_nm` | — | 주문 유형명 | 원본 외 추가 |
| ITEM_CD | `item_cd` | VARCHAR2(20) | 상품 코드 | |
| — | `item_nm` | — | 상품명 | 원본 외 추가 |
| ERP_SEND_DT | `erp_send_dt` | VARCHAR2(8) | ERP 전송 일자 | |
| ERP_WEB_ITEM_GRP | `erp_web_item_grp` | VARCHAR2(3) | ERP 웹 제품군 코드 | |
| — | `erp_web_item_grp_nm` | — | ERP 웹 제품군명 | 원본 외 추가 |
| ORD_UNIT | `ord_unit` | VARCHAR2(3) | 주문 단위 | |
| ORD_NOQQTY | `ord_noqqty` | NUMBER(7,0) | 주문 입수량 (낱개 환산: ×ord_qty) | |
| ORD_PRC | `ord_prc` | NUMBER(15,2) | 주문 단가 | |
| ORD_QTY | `ord_qty` | NUMBER(7,0) | **점주 실 주문 수량** | |
| ORD_AMT | `ord_amt` | NUMBER(15,2) | 주문 금액 | |
| ORD_VAT | `ord_vat` | NUMBER(15,2) | 주문 VAT | |
| CONFRM_PRC | `confrm_prc` | NUMBER(15,2) | 확정 단가 | |
| CONFRM_QTY | `confrm_qty` | NUMBER(7,0) | **확정 수량 (실 출하)** | 미출 = ord_qty - confrm_qty |
| CONFRM_AMT | `confrm_amt` | NUMBER(15,2) | 확정 금액 | |
| CONFRM_VAT | `confrm_vat` | NUMBER(15,2) | 확정 VAT | |
| CONFRM_DC_AMT | `confrm_dc_amt` | NUMBER(15,2) | 확정 할인 금액 | |
| AUTO_ORD_YN | `auto_ord_yn` | VARCHAR2(1) | 자동 주문 여부 | |
| ERP_DGRE | `erp_dgre` | VARCHAR2(2) | ERP 차수 | |
| — | `erp_dgre_nm` | — | ERP 차수명 | 원본 외 추가 |
| REG_USER_ID | — | VARCHAR2(50) | 등록자 ID | 미적재 |
| REG_DATE | — | DATE | 등록 일시 | 미적재 |
| UPD_USER_ID | — | VARCHAR2(50) | 수정자 ID | 미적재 |
| UPD_DATE | — | DATE | 수정 일시 | 미적재 |
| ORD_REC_QTY | `ord_rec_qty` | NUMBER(15,2) | **시스템 권고 주문 수량** (강제 아님) | |

---

## SPL_DAY_STOCK_DTL — 재고 테이블

> DB 테이블: `raw_inventory_extract`
>
> ⚠️ 컬럼명이 Oracle 원본과 다수 상이함. 일부 Oracle 컬럼 미적재, 원본 외 추가 컬럼 다수 존재.

| 원본 컬럼명 | DB 컬럼명 | 타입 | 설명 | 비고 |
|---|---|---|---|---|
| CMP_CD | `cmp_cd` | VARCHAR2 | 회사 코드 | |
| STOCK_DT | `stock_dt` | VARCHAR2 | 재고 일자 (YYYYMMDD) | |
| MASKED_STOR_CD | `masked_stor_cd` | VARCHAR2 | 마스킹 점포 코드 | |
| — | `masked_stor_nm` | — | 마스킹 점포명 | 원본 외 추가 |
| RECDIS_LOC | — | VARCHAR2 | 수불 위치 (M0049 참조) | 미적재 |
| ITEM_CD | `item_cd` | VARCHAR2 | 상품 코드 | |
| — | `item_nm` | — | 상품명 | 원본 외 추가 |
| BAR_CD | — | VARCHAR2 | 바코드 | 미적재 |
| GI_QTY | `gi_qty` | NUMBER | 입고 수량 | |
| GO_QTY | — | NUMBER | 출고 수량 | 미적재 |
| GO_CANCEL_QTY | — | NUMBER | 출고 취소 수량 | 미적재 |
| DSPS_QTY | `disuse_qty` | NUMBER | 폐기 수량 | 컬럼명 변경 |
| RTN_QTY | `rtn_qty` | NUMBER | 반품 수량 | |
| ADJ_QTY | `adj_qty` | NUMBER | 재고 조정 수량 | |
| MOVE_GI_QTY | `mv_in_qty` | NUMBER | 창고 이동 입고 수량 | 컬럼명 변경 |
| MOVE_GO_QTY | `mv_out_qty` | NUMBER | 창고 이동 출고 수량 | 컬럼명 변경 |
| CUST_RTN_QTY | — | NUMBER | 고객 반품 수량 | 미적재 |
| ADD_FOUT_QTY | `add_sout_qty` | NUMBER | 추가 선출 수량 | 컬럼명 변경 |
| ADD_UNDLV_QTY | `add_mout_qty` | NUMBER | 추가 미출 수량 | 컬럼명 변경 |
| ISPCTN_FOUT_QTY | `ins_sout_qty` | NUMBER | 검수 선출 수량 | 컬럼명 변경 |
| ISPCTN_UNDLV_QTY | `ins_mout_qty` | NUMBER | 검수 미출 수량 | 컬럼명 변경 |
| SALE_QTY | `sale_qty` | NUMBER | 판매 수량 | |
| DISTBT_EXPIRE | — | VARCHAR2 | 유통 기한 | 미적재 |
| REG_USER_ID | `reg_user_id` | VARCHAR2 | 등록자 ID | |
| REG_DATE | `reg_date` | DATE | 등록 일시 | |
| UPD_USER_ID | `upd_user_id` | VARCHAR2 | 수정자 ID | |
| UPD_DATE | `upd_date` | DATE | 수정 일시 | |
| NO_SALE_QTY | — | NUMBER | 비매출 수량 | 미적재 |
| — | `prod_in_qty` | — | 생산 입고 수량 | 원본 외 추가 |
| — | `prod_out_qty` | — | 생산 출고 수량 | 원본 외 추가 |
| — | `last_sale_dt` | — | 최근 판매 일자 | 원본 외 추가 |
| — | `cost` | — | 원가 | 원본 외 추가 |
| — | `sale_prc` | — | 판매 단가 | 원본 외 추가 |
| — | `sale_gram` | — | 판매 중량(g) | 원본 외 추가 |
| — | `stock_qty` | — | **재고 수량** (앱에서 현재고 기준으로 사용) | 원본 외 추가 |

---

## MST_TERM_COOP_CMP_PAY_DC — 마스터 기간 제휴사 결제 할인

> DB 테이블: `raw_telecom_discount_policy`
>
> 코드 컬럼에는 대응 `_nm` 컬럼이 추가되어 함께 적재됨

| 원본 컬럼명 | DB 컬럼명 | 설명 |
|---|---|---|
| CMP_CD | `cmp_cd` | 회사 코드 |
| PAY_DC_GRP_TYPE | `pay_dc_grp_type` | 결제 할인 그룹 유형 (M0077) |
| PAY_DC_CD | `pay_dc_cd` | 결제 할인 코드 |
| COOP_CMP_GRADE_CD | `coop_cmp_grade_cd` | 제휴사 등급 코드 |
| FUNC_ID | `func_id` | 기능키 코드 |
| START_DT | `start_dt` | 시작 일자 YYYYMMDD |
| FNSH_DT | `fnsh_dt` | 종료 일자 YYYYMMDD |
| DC_APPLY_TRGT | `dc_apply_trgt` | 할인 적용 대상 (M0078) |
| PAY_DC_METHD | `pay_dc_methd` | 결제 할인 방법 (M0079) |
| PAY_DC_VAL | `pay_dc_val` | 결제 할인 값 |
| PAY_DC_AMT_STD_PAY_AMT | `pay_dc_amt_std_pay_amt` | 결제 할인 금액 기준 결제 금액 |
| PAY_DC_DEC_PNT_CALC_METHD | `pay_dc_dec_pnt_calc_methd` | 소수점 계산 방법 (M0066) |
| PAY_DC_CALC_DIGT_NO | `pay_dc_calc_digt_no` | 계산 자릿수 |
| SALES_ORG_CD | `sales_org_cd` | 브랜드 코드 |
| FUNC_ID | `func_id` | 기능키 코드 |
| GRP_PRRTY | `grp_prrty` | 우선순위 (결제코드 기준) |
| GRADE_PRRTY | `grade_prrty` | 우선순위 (등급코드 기준) |
| ITEM_DC_YN | `item_dc_yn` | 할인 적용된 상품에 적용 여부 |
| USE_YN | `use_yn` | 사용 여부 |
| PAY_DC_AMT_MAX_PAY_AMT | `pay_dc_amt_max_pay_amt` | 최대 결제 할인값 |

---

## MST_TERM_COOP_CMP_DC_ITEM — 마스터 기간 제휴사 할인 상품

> DB 테이블: `raw_telecom_discount_item`
>
> 코드 컬럼에는 대응 `_nm` 컬럼이 추가되어 함께 적재됨. 상품 분류 컬럼 (`l/m/s_item_class_nm`) 원본 외 추가.

| 원본 컬럼명 | DB 컬럼명 | 설명 |
|---|---|---|
| CMP_CD | `cmp_cd` | 회사 코드 |
| PAY_DC_GRP_TYPE | `pay_dc_grp_type` | 결제 할인 그룹 유형 (M0077) |
| PAY_DC_CD | `pay_dc_cd` | 결제 할인 코드 |
| COOP_CMP_GRADE_CD | `coop_cmp_grade_cd` | 제휴사 등급 코드 |
| START_DT | `start_dt` | 시작 일자 YYYYMMDD |
| ITEM_CD | `item_cd` | 상품 코드 |
| USE_YN | `use_yn` | 사용 여부 (M0090) |
| SALES_ORG_CD | `sales_org_cd` | 브랜드 코드 |
| ITEM_SEQ | `item_seq` | 상품 우선순위 |

---

## MST_PAY_DC_INFO — 통신사 제휴 정산 기준 정보

> DB 테이블: `raw_settlement_master`
>
> **조인 규칙 (Info1)**: `SUBSTR(pay_dc_ty_cd, -2)` = `raw_pay_cd.pay_dtl_cd`
>
> **할인 방법 (Info2)**: `pay_dc_methd = 1` → 율 / `pay_dc_methd = 2` → 금액

| 원본 컬럼명 | DB 컬럼명 | 설명 |
|---|---|---|
| CMP_CD | `cmp_cd` | PK. 회사 코드 |
| SALES_ORG_CD | `sales_org_cd` | PK. 영업 조직 코드 (브랜드 코드) |
| PAY_DC_TY_CD | `pay_dc_ty_cd` | PK. 제휴통신사 결제코드 |
| COOP_CD | `coop_cd` | PK. 제휴통신사 제휴코드 |
| START_DT | `start_dt` | PK. 시작 일자 YYYYMMDD |
| FNSH_DT | `fnsh_dt` | PK. 종료 일자 YYYYMMDD |
| PAY_DC_METHD | `pay_dc_methd` | 결제 할인 방법 (1=율, 2=금액) |
| MAT_LIST | `mat_list` | 자재 내역 |
| HQ_ALLOT_RATE | `hq_allot_rate` | 본사 부담율 |
| STOR_ALLOT_RATE | `stor_allot_rate` | 점포 부담율 |
| COOP_CMP_ALLOT_RATE | `coop_cmp_allot_rate` | 제휴사 부담율 |
| HQ_VAT_YN | `hq_vat_yn` | 본사 레포트 부가세 적용 여부 |
| ERP_YN | `erp_yn` | ERP 정산 여부 |
| USE_YN | `use_yn` | 사용 여부 |
| REG_USER_ID | `reg_user_id` | 등록자 ID |
| REG_DATE | `reg_date` | 등록 일시 |
| UPD_USER_ID | `upd_user_id` | 수정자 ID |
| UPD_DATE | `upd_date` | 수정 일시 |

---

## CPI_MST — 캠페인 마스터

> DB 테이블: `raw_campaign_master`
>
> Oracle 컬럼명의 snake_case 변환이 기본 매핑이며, 대부분 1:1 대응됨.
> 코드 컬럼마다 대응 `_nm` 컬럼이 추가되어 함께 적재됨 (ex: `cpi_type` → `cpi_type_nm`).

**핵심 컬럼**

| 원본 컬럼명 | DB 컬럼명 | 설명 |
|---|---|---|
| CMP_CD | `cmp_cd` | 회사 코드 |
| SALES_ORG_CD | `sales_org_cd` | 영업 조직 코드 (브랜드 코드) |
| CPI_CD | `cpi_cd` | 캠페인 코드 |
| CPI_NM | `cpi_nm` | 캠페인명 |
| RPST_CPI_CD | `rpst_cpi_cd` | 대표 캠페인 코드 |
| PRGRS_STATUS | `prgrs_status` | 진행 상태 (E0004) |
| START_DT | `start_dt` | 시작 일자 YYYYMMDD |
| FNSH_DT | `fnsh_dt` | 종료 일자 YYYYMMDD |
| USE_YN | `use_yn` | 사용 여부 (M0090) |
| CPI_TYPE | `cpi_type` | 캠페인 유형 (E0003) |
| CPI_KIND | `cpi_kind` | 캠페인 종류 (E0002) |
| PRRTY | `prrty` | 우선순위 |
| ADMT_METHD | `admt_methd` | 정산 방법 (E0012) |
| TRGT_CUST_TYPE | `trgt_cust_type` | 대상 고객 유형 (E0005) |
| CPI_CUST_BNFT_TYPE | `cpi_cust_bnft_type` | 캠페인 고객 혜택 유형 (E0006) |
| REG_USER_ID | `reg_user_id` | 등록자 ID |
| REG_DATE | `reg_date` | 등록 일시 |
| UPD_USER_ID | `upd_user_id` | 수정자 ID |
| UPD_DATE | `upd_date` | 수정 일시 |

> 나머지 컬럼 (적립 정책, 부담율, 사은품, 복권, 수량 제한 등)은 Oracle 원본과 snake_case 매핑으로 전량 적재됨.

---

## CPI_ITEM_GRP_MNG — 캠페인 상품 그룹 관리

> DB 테이블: `raw_campaign_item_group`
>
> Oracle 컬럼명과 1:1 snake_case 매핑. 코드 컬럼에 대응 `_nm` 추가.

| 원본 컬럼명 | DB 컬럼명 | 설명 |
|---|---|---|
| CMP_CD | `cmp_cd` | 회사 코드 |
| SALES_ORG_CD | `sales_org_cd` | 영업 조직 코드 |
| CPI_CD | `cpi_cd` | 캠페인 코드 |
| CPI_ITEM_GRP_CD | `cpi_item_grp_cd` | 캠페인 상품 그룹 코드 |
| CPI_ITEM_GRP_NM | `cpi_item_grp_nm` | 캠페인 상품 그룹명 |
| CPI_COND_TYPE | `cpi_cond_type` | 캠페인 조건 유형 (E0031) |
| QTY_AMT | `qty_amt` | 수량/금액 |
| CPI_DC_TYPE | `cpi_dc_type` | 캠페인 할인 유형 (E0032) |
| DC_RATE_QTY_AMT | `dc_rate_qty_amt` | 할인율/수량/금액 |
| NOTE | `note` | 비고 |
| MAX_DC_AMT | `max_dc_amt` | 최대 할인 금액 |
| USE_YN | `use_yn` | 사용 여부 (M0090) |
| REG_USER_ID | `reg_user_id` | 등록자 ID |
| REG_DATE | `reg_date` | 등록 일시 |
| UPD_USER_ID | `upd_user_id` | 수정자 ID |
| UPD_DATE | `upd_date` | 수정 일시 |

---

## CPI_ITEM_MNG — 캠페인 상품 관리

> DB 테이블: `raw_campaign_item`
>
> Oracle 컬럼명과 1:1 snake_case 매핑. 코드 컬럼에 대응 `_nm` 추가. `item_nm` 원본 외 추가.

| 원본 컬럼명 | DB 컬럼명 | 설명 |
|---|---|---|
| CMP_CD | `cmp_cd` | 회사 코드 |
| SALES_ORG_CD | `sales_org_cd` | 영업 조직 코드 |
| CPI_CD | `cpi_cd` | 캠페인 코드 |
| CPI_ITEM_GRP_CD | `cpi_item_grp_cd` | 캠페인 상품 그룹 코드 |
| ITEM_LVL | `item_lvl` | 상품 레벨 (그룹/개별) |
| ITEM_CD | `item_cd` | 상품 코드 |
| CPI_DC_TYPE | `cpi_dc_type` | 캠페인 할인 유형 (E0032) |
| DC_RATE_AMT | `dc_rate_amt` | 할인율/금액 |
| USE_YN | `use_yn` | 사용 여부 (M0090) |
| REG_USER_ID | `reg_user_id` | 등록자 ID |
| REG_DATE | `reg_date` | 등록 일시 |
| UPD_USER_ID | `upd_user_id` | 수정자 ID |
| UPD_DATE | `upd_date` | 수정 일시 |

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
| `GET /api/ordering/options` | `raw_order_extract` 우선, 없으면 `core_daily_item_sales`, 최후 `raw_daily_store_item` | 주문 workbook 기반 상품/수량을 우선 계산한다. |
| `POST /api/ordering/selections` | `ordering_selections` | 점주의 최종 주문 선택을 저장한다. |
| `GET /api/ordering/selections/history` | `ordering_selections` | 저장된 주문 선택 이력을 조회한다. |
| `GET /api/ordering/selections/summary` | `ordering_selections` | 최근 주문 선택 상태와 요약 지표를 계산한다. |
| `GET /api/ordering/context/{notification_id}` | DB 직접 참조 없음 | 현재는 정적 컨텍스트 응답이다. |
| `GET /api/ordering/alerts` | DB 직접 참조 없음 | 현재는 서비스 로직 중심 알림 응답이다. |

### 생산 관리 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/production/overview` | `raw_production_extract` + `raw_inventory_extract` 우선, 없으면 `core_hourly_item_sales`, 최후 `raw_daily_store_item_tmzon` | 생산/재고 workbook 데이터를 우선 참조해 생산 대상 품목을 계산한다. |
| `GET /api/production/items`, `GET /api/production/skus` | `raw_production_extract` + `raw_inventory_extract` 우선, 없으면 `core_hourly_item_sales` | SKU별 재고·예측·권장 수량 목록을 반환한다. |
| `GET /api/production/alerts` | `core_hourly_item_sales` / `raw_daily_store_item_tmzon` 기반 계산 | 생산 위험 SKU를 서비스 로직으로 도출한다. |
| `POST /api/production/registrations` | `production_registrations` | 생산 등록과 피드백 결과를 저장한다. |
| `GET /api/production/registrations/history` | `production_registrations` | 생산 등록 이력을 조회한다. |
| `GET /api/production/registrations/summary` | `production_registrations` | 최근 생산 등록 요약 지표를 계산한다. |

### 매출 분석 API

| API | 주요 참조 대상 | 설명 |
|------|------|------|
| `GET /api/sales/prompts` | DB 직접 참조 없음 | 추천 질문 목록은 현재 코드상 정적 데이터다. |
| `POST /api/sales/query` | `core_channel_sales` 우선, 없으면 `raw_daily_store_online` | 배달/온라인 관련 질의는 채널 매출 데이터를 우선 사용한다. 그 외 질의는 스텁 응답 또는 서비스 로직을 사용한다. |
| `GET /api/sales/insights` | `core_hourly_item_sales`, `core_channel_sales`, `raw_daily_store_pay_way`, `core_daily_item_sales`, `raw_campaign_master`, `raw_campaign_item_group`, `raw_campaign_item`, `raw_settlement_master`, `raw_telecom_discount_policy` | 피크타임, 채널 믹스, 결제 믹스, 메뉴 믹스, 캠페인 시즌성 인사이트를 각각 다른 소스에서 계산한다. |

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
- `GET /api/analytics/market-intelligence`는 `raw_seoul_market_sales` + `raw_seoul_market_floating_population` 실데이터를 기준으로 응답하며, `household_composition_pie`/`estimated_residence_regions`도 동일 테이블 집계(추정 포함)로 생성한다.
- 요청한 `year/quarter`에 데이터가 없을 때는 동일 스코프에서 연도/분기 조건만 제거한 가용 실데이터로 폴백한다(합성값 사용 없음).
- 동일 API에서 `industry_analysis`/`sales_analysis`/`population_analysis`/`regional_status`/`customer_characteristics`를 함께 생성하며, 일부 항목은 원천 컬럼 한계로 reference 기반 프록시 집계(예: 직장·주거 인구, 소득소비, 교통접근지수)를 사용한다.
- `customer_characteristics.new_customer_ratio/regular_customer_ratio`는 `raw_daily_store_item/raw_daily_store_pay_way/raw_order_extract`의 고객식별 컬럼(존재 시)을 자동탐지해 계산하고, 미존재 시 `raw_daily_store_cpi_tmzon` 키워드 보조추출을 시도한 뒤 최종적으로 null 처리한다.
