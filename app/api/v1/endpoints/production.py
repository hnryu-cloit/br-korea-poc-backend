from fastapi import APIRouter, Depends

from app.core.deps import get_production_service
from app.schemas.production import (
    ProductionOverviewResponse,
    ProductionRegistrationRequest,
    ProductionRegistrationResponse,
)
from app.services.production_service import ProductionService

router = APIRouter(prefix="/production", tags=["production"])


@router.get("/overview", response_model=ProductionOverviewResponse)
async def get_production_overview(
    service: ProductionService = Depends(get_production_service),
) -> ProductionOverviewResponse:
    return await service.get_overview()


@router.post("/registrations", response_model=ProductionRegistrationResponse)
async def register_production(
    payload: ProductionRegistrationRequest,
    service: ProductionService = Depends(get_production_service),
) -> ProductionRegistrationResponse:
    return await service.register_production(payload)
