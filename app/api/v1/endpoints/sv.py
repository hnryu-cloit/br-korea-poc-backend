from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/sv", tags=["sv"])


class StoreOrderItem(BaseModel):
    store: str
    region: str
    option: str
    basis: str
    reason: str
    submitted_at: str
    status: str


class CoachingTip(BaseModel):
    store: str
    tip: str


class SvCoachingResponse(BaseModel):
    store_orders: list[StoreOrderItem]
    coaching_tips: list[CoachingTip]


class StoreInspectionItem(BaseModel):
    store: str
    region: str
    alert_response_rate: int
    production_registered: int
    production_total: int
    chance_loss_change: str
    status: str


class SvInspectionResponse(BaseModel):
    items: list[StoreInspectionItem]


_COACHING_ORDERS: list[StoreOrderItem] = [
    StoreOrderItem(store="강남 1호점", region="강남구", option="옵션 A", basis="전주 동요일", reason="캠페인 영향 감안", submitted_at="13:42", status="normal"),
    StoreOrderItem(store="서초 2호점", region="서초구", option="옵션 C", basis="전월 동요일", reason="시즌 수요 반영", submitted_at="13:55", status="review"),
    StoreOrderItem(store="마포 3호점", region="마포구", option="옵션 B", basis="전전주 동요일", reason="과주문 방지", submitted_at="14:01", status="normal"),
    StoreOrderItem(store="송파 4호점", region="송파구", option="-", basis="-", reason="-", submitted_at="-", status="risk"),
    StoreOrderItem(store="용산 5호점", region="용산구", option="옵션 A", basis="전주 동요일", reason="무난한 선택", submitted_at="13:38", status="normal"),
]

_COACHING_TIPS: list[CoachingTip] = [
    CoachingTip(store="서초 2호점", tip="전월 동요일 선택 시 배달 회복분 보정 필요. 실제 주문은 -12% 적게 반영됩니다."),
    CoachingTip(store="송파 4호점", tip="주문 마감까지 5분 남음. 점주에게 긴급 알림 발송 필요."),
]

_INSPECTIONS: list[StoreInspectionItem] = [
    StoreInspectionItem(store="강남 1호점", region="강남구", alert_response_rate=100, production_registered=8, production_total=8, chance_loss_change="-12%", status="compliant"),
    StoreInspectionItem(store="서초 2호점", region="서초구", alert_response_rate=75, production_registered=5, production_total=8, chance_loss_change="+4%", status="partial"),
    StoreInspectionItem(store="마포 3호점", region="마포구", alert_response_rate=100, production_registered=7, production_total=8, chance_loss_change="-9%", status="compliant"),
    StoreInspectionItem(store="송파 4호점", region="송파구", alert_response_rate=50, production_registered=3, production_total=8, chance_loss_change="+18%", status="noncompliant"),
    StoreInspectionItem(store="용산 5호점", region="용산구", alert_response_rate=88, production_registered=6, production_total=8, chance_loss_change="-6%", status="compliant"),
]


@router.get("/coaching", response_model=SvCoachingResponse)
async def get_sv_coaching() -> SvCoachingResponse:
    return SvCoachingResponse(store_orders=_COACHING_ORDERS, coaching_tips=_COACHING_TIPS)


@router.get("/inspection", response_model=SvInspectionResponse)
async def get_sv_inspection() -> SvInspectionResponse:
    return SvInspectionResponse(items=_INSPECTIONS)