from fastapi import APIRouter, Depends
from app.core.deps import get_production_service
from app.services.production_service import ProductionService

from app.core.deps import get_home_service
from app.schemas.home import HomeOverviewRequest, HomeOverviewResponse
from app.services.home_service import HomeService

router = APIRouter(prefix="/home", tags=["home"])


@router.post("/overview", response_model=HomeOverviewResponse)
async def get_home_overview(
    payload: HomeOverviewRequest,
    service: HomeService = Depends(get_home_service),
) -> HomeOverviewResponse:
    return await service.get_overview(payload)
