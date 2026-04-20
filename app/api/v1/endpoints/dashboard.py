from fastapi import APIRouter, Depends, Query

from app.core.deps import get_home_service
from app.schemas.home import HomeOverviewRequest, HomeOverviewResponse
from app.services.home_service import HomeService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/overview", response_model=HomeOverviewResponse)
async def get_dashboard_overview(
    store_id: str | None = Query(default=None),
    business_date: str | None = Query(default=None),
    service: HomeService = Depends(get_home_service),
) -> HomeOverviewResponse:
    return await service.get_overview(
        HomeOverviewRequest(store_id=store_id, business_date=business_date)
    )
