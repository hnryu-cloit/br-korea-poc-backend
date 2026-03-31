"""
DB 테이블 기반 Pydantic 스키마 정의
출처: POC 대상 테이블 구조(작성중)_V3.xlsx
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# STOR_MST — 점포 마스터
# ---------------------------------------------------------------------------

class StorMst(BaseModel):
    masked_stor_cd: str           # 점포코드
    masked_stor_nm: str           # 점포명


# ---------------------------------------------------------------------------
# PAY_CD — 결제수단 코드
# ---------------------------------------------------------------------------

class PayCd(BaseModel):
    pay_way_cd: str               # 결제/할인 그룹코드
    pay_way_nm: str               # 결제/할인 그룹명
    pay_dtl_cd: str               # 결제/할인 코드
    pay_dtl_nm: str               # 결제/할인 코드명
    pay_dc_type: str              # 결제/할인 구분
    pay_dc_type_nm: str           # 결제/할인 구분명


# ---------------------------------------------------------------------------
# DAILY_STOR_ITEM_TMZON — 일자·시간대·상품별 매출
# ---------------------------------------------------------------------------
# 비고:
#   ACTUAL_SALE_AMT = SALE_AMT - DC_AMT
#   NET_SALE_AMT    = SALE_AMT - DC_AMT - VAT_AMT
#   부가세율: 10%

class DailyStorItemTmzon(BaseModel):
    masked_stor_cd: str           # 점포코드
    masked_stor_nm: str           # 점포명
    item_cd: Optional[str]        # 상품코드
    item_nm: str                  # 상품명
    sale_dt: str                  # 판매일자 (YYYYMMDD)
    tmzon_div: str                # 시간대 구분
    sale_qty: int                 # 판매수량
    sale_amt: int                 # 판매금액
    rtn_qty: int = 0              # 반품수량
    rtn_amt: int = 0              # 반품금액
    dc_amt: int = 0               # 할인금액
    enuri_amt: int = 0            # 에누리금액
    vat_amt: int = 0              # 부가세금액
    actual_sale_amt: int = 0      # 실매출금액 (= SALE_AMT - DC_AMT)
    net_sale_amt: int = 0         # 순매출금액 (= SALE_AMT - DC_AMT - VAT_AMT)
    take_in_amt: int = 0          # TAKE IN 금액
    take_in_vat_amt: int = 0      # TAKE IN 부가세
    take_out_amt: int = 0         # TAKE OUT 금액
    take_out_vat_amt: int = 0     # TAKE OUT 부가세
    svc_fee_amt: int = 0          # 봉사료 금액
    svc_fee_vat_amt: int = 0      # 봉사료 부가세
    reg_user_id: Optional[str]    # 등록자 ID
    reg_date: Optional[datetime]  # 등록일시
    upd_user_id: Optional[str]    # 수정자 ID
    upd_date: Optional[datetime]  # 수정일시


# ---------------------------------------------------------------------------
# DAILY_STOR_CPI_TMZON — 일자별 시간대별 캠페인 매출
# ---------------------------------------------------------------------------

class DailyStorCpiTmzon(BaseModel):
    cmp_cd: str                       # 회사코드
    sale_dt: str                      # 판매일자 (YYYYMMDD)
    masked_stor_cd: str               # 점포코드
    cpi_cd: str                       # 캠페인코드
    cpi_add_accum_point: float = 0    # 캠페인 추가 적립 포인트
    cpi_cust_use_point: float = 0     # 캠페인 고객 사용 포인트
    cpi_dc_qty: int = 0               # 캠페인 할인 수량
    cpi_dc_amt: float = 0             # 캠페인 할인 금액
    cpi_custcnt: float = 0            # 캠페인 고객수
    cpi_billcnt: float = 0            # 캠페인 영수건수
    totsale_qty: int = 0              # 총 판매수량
    totsale_amt: float = 0            # 총 판매금액
    totdc_amt: float = 0              # 총 할인금액
    totactual_sale_amt: float = 0     # 총 실매출금액
    totvat_amt: float = 0             # 총 부가세
    totnet_sale_amt: float = 0        # 총 순매출금액
    totbillcnt: float = 0             # 총 영수건수
    totcustcnt: float = 0             # 총 고객수
    reg_user_id: Optional[str]        # 등록자 ID
    reg_date: Optional[datetime]      # 등록일시
    upd_user_id: Optional[str]        # 수정자 ID
    upd_date: Optional[datetime]      # 수정일시


# ---------------------------------------------------------------------------
# DAILY_STOR_PAY_WAY — 일자별 결제수단별 매출
# ---------------------------------------------------------------------------
# PAY_WAY_CD 코드표:
#   00:현금, 01:수표, 02:신용카드, 03:제휴할인(통신사), 04:포인트사용
#   06:상품권, 07:알리페이, 08:쿠폰, 09:모바일CASH, 10:선불카드
#   11:모바일CON, 12:직원결제, 13:외상, 14:외화, 15:예약
#   16:직원할인, 17:임의할인, 99:기타결제, 포인트적립(01)

class DailyStorPayWay(BaseModel):
    cmp_cd: str                           # 회사코드
    sale_dt: str                          # 판매일자 (YYYYMMDD)
    masked_stor_cd: str                   # 점포코드
    pay_way_cd: str                       # 결제수단코드
    pay_dtl_cd: str                       # 결제 세부코드
    pay_amt: float = 0                    # 결제금액
    rec_amt: float = 0                    # 받은금액
    change: float = 0                     # 거스름돈
    rtn_pay_amt: float = 0               # 반품 결제금액
    rtn_rec_amt: float = 0               # 반품 받은금액
    rtn_change: float = 0                 # 반품 거스름돈
    etc_profit_amt: float = 0             # 기타 수익금액
    rtn_etc_profit_amt: float = 0         # 반품 기타 수익금액
    cash_exchng_cpn: float = 0            # 현금 교환권
    rtn_cash_exchng_cpn: float = 0        # 반품 현금 교환권
    reg_user_id: Optional[str]            # 등록자 ID
    reg_date: Optional[datetime]          # 등록일시
    upd_user_id: Optional[str]            # 수정자 ID
    upd_date: Optional[datetime]          # 수정일시


# ---------------------------------------------------------------------------
# DAILY_STOR_ITEM — 일자별 상품별 매출
# ---------------------------------------------------------------------------
# ITEM_TAX_DIV: 상품 과세 구분 (M0018 코드표 참조)

class DailyStorItem(BaseModel):
    cmp_cd: str                       # 회사코드
    sale_dt: str                      # 판매일자 (YYYYMMDD)
    masked_stor_cd: str               # 점포코드
    item_cd: str                      # 상품코드
    item_tax_div: Optional[str]       # 상품 과세 구분
    sale_qty: int = 0                 # 판매수량
    sale_amt: float = 0               # 판매금액
    rtn_qty: int = 0                  # 반품수량
    rtn_amt: float = 0                # 반품금액
    dc_amt: float = 0                 # 할인금액
    enuri_amt: float = 0              # 에누리금액
    vat_amt: float = 0                # 부가세금액
    actual_sale_amt: float = 0        # 실매출금액
    net_sale_amt: float = 0           # 순매출금액
    take_in_amt: float = 0            # TAKE IN 금액
    take_in_vat_amt: float = 0        # TAKE IN 부가세
    take_out_amt: float = 0           # TAKE OUT 금액
    take_out_vat_amt: float = 0       # TAKE OUT 부가세
    svc_fee_amt: float = 0            # 봉사료 금액
    svc_fee_vat_amt: float = 0        # 봉사료 부가세
    reg_user_id: Optional[str]        # 등록자 ID
    reg_date: Optional[datetime]      # 등록일시
    upd_user_id: Optional[str]        # 수정자 ID
    upd_date: Optional[datetime]      # 수정일시


# ---------------------------------------------------------------------------
# DAILY_STOR_ONLINE — 일자별 온/오프라인 매출
# ---------------------------------------------------------------------------
# HO_CHNL_DIV: 판매유형 (온라인 / 오프라인)

class DailyStorOnline(BaseModel):
    masked_stor_cd: str           # 점포코드
    masked_stor_nm: str           # 점포명
    sale_dt: str                  # 판매일자 (YYYYMMDD)
    tmzon_div: str                # 판매시간대
    ho_chnl_cd: str               # 판매채널코드
    ho_chnl_nm: str               # 판매채널명
    sales_org_nm: str             # 영업조직
    ho_chnl_div: str              # 판매유형 (온/오프라인)
    sale_amt: float = 0           # 판매금액
    ord_cnt: int = 0              # 판매수량