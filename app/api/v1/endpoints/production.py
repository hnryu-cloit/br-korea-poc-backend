from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import get_production_service
from app.schemas.production import (
    GetProductionSkuListResponse,
    InventoryStatusResponse,
    ProductionAlertsResponse,
    ProductionOverviewResponse,
    ProductionRegistrationFormResponse,
    ProductionRegistrationHistoryResponse,
    ProductionRegistrationRequest,
    ProductionRegistrationResponse,
    ProductionRegistrationSummaryResponse,
    ProductionSimulationRequest,
    ProductionSimulationResponse,
    ProductionSkuDetailResponse,
    WasteSummaryResponse,
)
from app.services.production_service import ProductionService

router = APIRouter(prefix="/production", tags=["production"])
v1_router = APIRouter(prefix="/v1/production", tags=["production"])


@router.get("/overview", response_model=ProductionOverviewResponse)
async def get_production_overview(
    store_id: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionOverviewResponse:
    return await service.get_overview(store_id=store_id)


@router.get("/skus", response_model=GetProductionSkuListResponse)
@router.get("/items", response_model=GetProductionSkuListResponse)
async def get_production_sku_list(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=100),
    store_id: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> GetProductionSkuListResponse:
    return await service.get_sku_list(page=page, page_size=page_size, store_id=store_id)


@router.get("/items/{sku_id}", response_model=ProductionSkuDetailResponse)
async def get_production_sku_detail(
    sku_id: str,
    store_id: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionSkuDetailResponse:
    try:
        return await service.get_sku_detail(sku_id=sku_id, store_id=store_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/registrations/form", response_model=ProductionRegistrationFormResponse)
async def get_production_registration_form(
    store_id: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationFormResponse:
    return await service.get_registration_form(store_id=store_id)


@router.get("/alerts", response_model=ProductionAlertsResponse)
async def get_production_alerts(
    store_id: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionAlertsResponse:
    return await service.get_alerts(store_id=store_id)


@router.post("/registrations", response_model=ProductionRegistrationResponse)
async def register_production(
    payload: ProductionRegistrationRequest,
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationResponse:
    return await service.register_production(payload)


@router.get("/registrations/history", response_model=ProductionRegistrationHistoryResponse)
async def list_production_registration_history(
    limit: int = Query(default=20, ge=1, le=100),
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
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
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationSummaryResponse:
    return await service.get_registration_summary(
        store_id=store_id, date_from=date_from, date_to=date_to
    )


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


@router.get("/waste-summary", response_model=WasteSummaryResponse)
async def get_waste_summary(
    store_id: str = Query(..., min_length=1),
    service: ProductionService = Depends(get_production_service),
) -> WasteSummaryResponse:
    try:
        return await service.get_waste_summary(store_id=store_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"폐기 요약 조회 오류: {str(exc)}") from exc


@router.get("/alerts/push", response_model=ProductionAlertsResponse)
async def get_production_push_alerts(
    store_id: str | None = Query(default=None),
    service: ProductionService = Depends(get_production_service),
) -> ProductionAlertsResponse:
    return await service.get_alerts(store_id=store_id)


@router.get("/inventory-status", response_model=InventoryStatusResponse)
async def get_inventory_status(
    store_id: str = Query(..., min_length=1),
    service: ProductionService = Depends(get_production_service),
) -> InventoryStatusResponse:
    try:
        return await service.get_inventory_status(store_id=store_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"재고 상태 조회 오류: {str(exc)}") from exc
