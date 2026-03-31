from fastapi import APIRouter, Depends, Query

from app.core.deps import get_ordering_service
from app.schemas.ordering import (
    OrderingContextResponse,
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


@router.post("/selections", response_model=OrderSelectionResponse)
async def save_order_selection(
    payload: OrderSelectionRequest,
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionResponse:
    return await service.save_selection(payload)
