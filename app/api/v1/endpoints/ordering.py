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
    store_id: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingOptionsResponse:
    return await service.list_options(notification_entry=notification_entry, store_id=store_id)


@router.get("/context/{notification_id}", response_model=OrderingContextResponse)
async def get_ordering_context(
    notification_id: int,
    store_id: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingContextResponse:
    return await service.get_notification_context(notification_id, store_id=store_id)


@router.get("/alerts", response_model=OrderingAlertsResponse)
async def list_ordering_alerts(
    before_minutes: int = Query(default=20, ge=1, le=120),
    store_id: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingAlertsResponse:
    return await service.list_deadline_alerts(before_minutes=before_minutes, store_id=store_id)


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
    return await service.get_deadline(store_id=store_id)


@router.get("/selections/summary", response_model=OrderSelectionSummaryResponse)
async def get_order_selection_summary(
    store_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionSummaryResponse:
    return await service.get_selection_summary(store_id=store_id, date_from=date_from, date_to=date_to)
