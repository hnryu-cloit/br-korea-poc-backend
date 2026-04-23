from fastapi import APIRouter, Depends, Query

from app.core.deps import get_dashboard_service
from app.schemas.dashboard import (
    DashboardAlertsResponse,
    DashboardHomeRequest,
    DashboardNoticesResponse,
    DashboardSummaryCardsResponse,
)
from app.services.dashboard_service import DashboardService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/notices", response_model=DashboardNoticesResponse)
async def get_dashboard_notices(
    store_id: str | None = Query(default=None),
    business_date: str | None = Query(default=None),
    service: DashboardService = Depends(get_dashboard_service),
) -> DashboardNoticesResponse:
    return await service.get_notices(
        DashboardHomeRequest(store_id=store_id, business_date=business_date)
    )


@router.get("/alerts", response_model=DashboardAlertsResponse)
async def get_dashboard_alerts(
    store_id: str | None = Query(default=None),
    business_date: str | None = Query(default=None),
    service: DashboardService = Depends(get_dashboard_service),
) -> DashboardAlertsResponse:
    return await service.get_alerts(
        DashboardHomeRequest(store_id=store_id, business_date=business_date)
    )


@router.get("/summary-cards", response_model=DashboardSummaryCardsResponse)
async def get_dashboard_summary_cards(
    store_id: str | None = Query(default=None),
    business_date: str | None = Query(default=None),
    service: DashboardService = Depends(get_dashboard_service),
) -> DashboardSummaryCardsResponse:
    return await service.get_summary_cards(
        DashboardHomeRequest(store_id=store_id, business_date=business_date)
    )
