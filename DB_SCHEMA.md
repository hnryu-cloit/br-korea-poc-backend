# DB 테이블 스키마 정의

> 출처: `POC 대상 테이블 구조(작성중)_V3.xlsx`
> 구현: FastAPI + Pydantic (`app/schemas/db_schemas.py`)

---

## 테이블 목록

| 테이블명 | 시트명 | 설명 |
|----------|--------|------|
| `STOR_MST` | 점포 마스터 | 점포 기본 정보 |
| `PAY_CD` | 결제수단 코드 | 결제/할인 코드 마스터 |
| `DAILY_STOR_ITEM_TMZON` | 일자·시간대·상품별 매출 | 시간대별 상품 매출 상세 |
| `DAILY_STOR_CPI_TMZON` | 일자별 시간대별 캠페인 매출 | 캠페인별 매출 집계 |
| `DAILY_STOR_PAY_WAY` | 일자별 결제수단별 매출 | 결제수단별 매출 집계 |
| `DAILY_STOR_ITEM` | 일자별 상품별 매출 | 상품별 일자 매출 집계 |
| `DAILY_STOR_ONLINE` | 일자별 온/오프라인 매출 | 채널(온/오프라인)별 매출 |

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