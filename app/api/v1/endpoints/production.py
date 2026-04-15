from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import get_production_service
from app.schemas.production import (
    ProductionAlertsResponse,
    ProductionOverviewResponse,
    ProductionRegistrationHistoryResponse,
    ProductionRegistrationRequest,
    ProductionRegistrationSummaryResponse,
    ProductionRegistrationResponse,
    ProductionSimulationRequest,
    ProductionSimulationResponse,
    GetProductionSkuListResponse,
    ProductionSkuDetailResponse,
)
from app.services.production_service import ProductionService

router = APIRouter(prefix="/production", tags=["production"])
v1_router = APIRouter(prefix="/v1/production", tags=["production"])


@router.get("/overview", response_model=ProductionOverviewResponse)
async def get_production_overview(
    service: ProductionService = Depends(get_production_service),
) -> ProductionOverviewResponse:
    return await service.get_overview()


@router.get("/skus", response_model=GetProductionSkuListResponse)
@router.get("/items", response_model=GetProductionSkuListResponse)
async def get_production_sku_list(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    store_id: Optional[str] = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> GetProductionSkuListResponse:
    return await service.get_sku_list(page=page, page_size=page_size, store_id=store_id)


@router.get("/items/{sku_id}", response_model=ProductionSkuDetailResponse)
async def get_production_sku_detail(
    sku_id: str,
    store_id: Optional[str] = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionSkuDetailResponse:
    try:
        return await service.get_sku_detail(sku_id=sku_id, store_id=store_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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


@router.post("/simulation", response_model=ProductionSimulationResponse)
@v1_router.post("/simulation", response_model=ProductionSimulationResponse)
async def run_production_simulation(
    payload: ProductionSimulationRequest,
    service: ProductionService = Depends(get_production_service),
) -> ProductionSimulationResponse:
    try:
        return await service.run_simulation(payload)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"시뮬레이션 실행 오류: {str(exc)}",
        ) from exc
