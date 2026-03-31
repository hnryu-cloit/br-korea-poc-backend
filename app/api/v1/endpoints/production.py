from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_production_service
from app.schemas.production import (
    ProductionAlertsResponse,
    ProductionOverviewResponse,
    ProductionRegistrationHistoryResponse,
    ProductionRegistrationRequest,
    ProductionRegistrationSummaryResponse,
    ProductionRegistrationResponse,
)
from app.services.production_service import ProductionService

router = APIRouter(prefix="/production", tags=["production"])


@router.get("/overview", response_model=ProductionOverviewResponse)
async def get_production_overview(
    service: ProductionService = Depends(get_production_service),
) -> ProductionOverviewResponse:
    return await service.get_overview()


@router.get("/alerts", response_model=ProductionAlertsResponse)
async def get_production_alerts(
    service: ProductionService = Depends(get_production_service),
) -> ProductionAlertsResponse:
    return await service.get_alerts()


@router.post("/registrations", response_model=ProductionRegistrationResponse)
async def register_production(
    payload: ProductionRegistrationRequest,
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationResponse:
    return await service.register_production(payload)


@router.get("/registrations/history", response_model=ProductionRegistrationHistoryResponse)
async def list_production_registration_history(
    limit: int = Query(default=20, ge=1, le=100),
    store_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationHistoryResponse:
    return await service.list_registration_history(
        limit=limit,
        store_id=store_id,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/registrations/summary", response_model=ProductionRegistrationSummaryResponse)
async def get_production_registration_summary(
    store_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationSummaryResponse:
    return await service.get_registration_summary(store_id=store_id, date_from=date_from, date_to=date_to)
