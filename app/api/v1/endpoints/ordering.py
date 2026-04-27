from fastapi import APIRouter, Depends, Header, HTTPException, Query

from app.core.reference_datetime import parse_reference_datetime
from app.core.deps import get_ordering_service
from app.schemas.ordering import (
    OrderingActiveCampaignsResponse,
    OrderingAlertsResponse,
    OrderingHistoryChartsResponse,
    OrderingContextResponse,
    OrderingHistoryResponse,
    OrderingHistoryInsightsResponse,
    OrderingOptionsResponse,
    OrderSelectionHistoryResponse,
    OrderSelectionRequest,
    OrderSelectionResponse,
    OrderSelectionSummaryResponse,
)
from app.services.ordering_service import OrderingService

router = APIRouter(prefix="/ordering", tags=["ordering"])
DEFAULT_ORDERING_REFERENCE_DATETIME = "2026-03-05T09:00:00+09:00"


def _resolve_ordering_reference_datetime(
    x_reference_datetime: str | None,
):
    return parse_reference_datetime(
        x_reference_datetime or DEFAULT_ORDERING_REFERENCE_DATETIME
    )


@router.get("/options", response_model=OrderingOptionsResponse)
async def list_order_options(
    notification_entry: bool = Query(default=False),
    store_id: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingOptionsResponse:
    return await service.list_options(
        notification_entry=notification_entry,
        store_id=store_id,
        reference_datetime=_resolve_ordering_reference_datetime(x_reference_datetime),
    )


@router.get("/active-campaigns", response_model=OrderingActiveCampaignsResponse)
def list_ordering_active_campaigns(
    reference_date: str | None = Query(default=None, description="YYYY-MM-DD 또는 YYYYMMDD"),
    limit: int = Query(default=3, ge=1, le=10),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingActiveCampaignsResponse:
    return service.list_active_campaigns(reference_date=reference_date, limit=limit)


@router.get("/context/{notification_id}", response_model=OrderingContextResponse)
async def get_ordering_context(
    notification_id: int,
    store_id: str | None = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingContextResponse:
    return await service.get_notification_context(notification_id, store_id=store_id)


@router.get("/alerts", response_model=OrderingAlertsResponse)
async def list_ordering_alerts(
    before_minutes: int = Query(default=20, ge=1, le=120),
    store_id: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingAlertsResponse:
    return await service.list_deadline_alerts(
        before_minutes=before_minutes,
        store_id=store_id,
        reference_datetime=_resolve_ordering_reference_datetime(x_reference_datetime),
    )


@router.post("/selections", response_model=OrderSelectionResponse)
async def save_order_selection(
    payload: OrderSelectionRequest,
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionResponse:
    return await service.save_selection(payload)


@router.get("/selections/history", response_model=OrderSelectionHistoryResponse)
async def list_order_selection_history(
    limit: int = Query(default=20, ge=1, le=100),
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionHistoryResponse:
    return await service.list_selection_history(
        limit=limit, store_id=store_id, date_from=date_from, date_to=date_to
    )


@router.get("/deadline")
async def get_ordering_deadline(
    store_id: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: OrderingService = Depends(get_ordering_service),
) -> dict:
    return await service.get_deadline(
        store_id=store_id,
        reference_datetime=_resolve_ordering_reference_datetime(x_reference_datetime),
    )


@router.get("/selections/summary", response_model=OrderSelectionSummaryResponse)
async def get_order_selection_summary(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderSelectionSummaryResponse:
    return await service.get_selection_summary(
        store_id=store_id, date_from=date_from, date_to=date_to
    )


@router.get("/history", response_model=OrderingHistoryResponse)
def get_ordering_history(
    store_id: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    item_nm: str | None = Query(default=None),
    is_auto: bool | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingHistoryResponse:
    try:
        return service.get_history(
            store_id=store_id,
            limit=limit,
            page=page,
            page_size=page_size,
            date_from=date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            reference_datetime=_resolve_ordering_reference_datetime(x_reference_datetime),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/history/insights", response_model=OrderingHistoryInsightsResponse)
async def get_ordering_history_insights(
    store_id: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    item_nm: str | None = Query(default=None),
    is_auto: bool | None = Query(default=None),
    limit: int = Query(default=200, ge=20, le=500),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingHistoryInsightsResponse:
    try:
        return await service.get_history_insights(
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            limit=limit,
            reference_datetime=_resolve_ordering_reference_datetime(x_reference_datetime),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=502,
            detail="AI ordering insights generation failed",
        ) from exc


@router.get("/history/charts", response_model=OrderingHistoryChartsResponse)
def get_ordering_history_charts(
    store_id: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    item_nm: str | None = Query(default=None),
    is_auto: bool | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: OrderingService = Depends(get_ordering_service),
) -> OrderingHistoryChartsResponse:
    try:
        return service.get_history_charts(
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            reference_datetime=_resolve_ordering_reference_datetime(x_reference_datetime),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
