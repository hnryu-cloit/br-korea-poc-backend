from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_ordering_service
from app.schemas.ordering import (
    OrderingAlertsResponse,
    OrderingContextResponse,
    OrderSelectionHistoryResponse,
    OrderSelectionSummaryResponse,
    OrderingOptionsResponse,
    OrderSelectionRequest,
    OrderSelectionResponse,
)
from app.services.ordering_service import OrderingService

router = APIRouter(prefix="/ordering", tags=["ordering"])


@router.get("/options", response_model=OrderingOptionsResponse)
async def list_order_options(
    notification_entry: bool = Query(default=False),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingOptionsResponse:
    return await service.list_options(notification_entry=notification_entry)


@router.get("/context/{notification_id}", response_model=OrderingContextResponse)
async def get_ordering_context(
    notification_id: int,
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingContextResponse:
    return await service.get_notification_context(notification_id)


@router.get("/alerts", response_model=OrderingAlertsResponse)
async def list_ordering_alerts(
    before_minutes: int = Query(default=20, ge=1, le=120),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingAlertsResponse:
    return await service.list_deadline_alerts(before_minutes=before_minutes)


@router.post("/selections", response_model=OrderSelectionResponse)
async def save_order_selection(
    payload: OrderSelectionRequest,
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionResponse:
    return await service.save_selection(payload)


@router.get("/selections/history", response_model=OrderSelectionHistoryResponse)
async def list_order_selection_history(
    limit: int = Query(default=20, ge=1, le=100),
    store_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionHistoryResponse:
    return await service.list_selection_history(limit=limit, store_id=store_id, date_from=date_from, date_to=date_to)


@router.get("/deadline")
async def get_ordering_deadline(
    store_id: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> dict:
    """주문 마감까지 남은 시간 정보를 반환합니다."""
    from datetime import datetime
    try:
        import pytz
        KST = pytz.timezone("Asia/Seoul")
        now = datetime.now(KST)
    except ImportError:
        now = datetime.utcnow()

    deadline = now.replace(hour=14, minute=0, second=0, microsecond=0)
    delta = int((deadline - now).total_seconds() / 60)
    sid = store_id or "gangnam"
    return {
        "store_id": sid,
        "deadline": "14:00",
        "minutes_remaining": max(0, delta),
        "is_urgent": 0 <= delta <= 20,
        "is_passed": delta < 0,
    }


@router.get("/selections/summary", response_model=OrderSelectionSummaryResponse)
async def get_order_selection_summary(
    store_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionSummaryResponse:
    return await service.get_selection_summary(store_id=store_id, date_from=date_from, date_to=date_to)
