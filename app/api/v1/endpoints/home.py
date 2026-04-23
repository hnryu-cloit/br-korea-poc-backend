from fastapi import APIRouter, Depends, Query

from app.core.deps import get_home_service
from app.schemas.home import DashboardHomeRequest, ScheduleResponse
from app.services.home_service import HomeService

router = APIRouter(prefix="/home", tags=["home"])

@router.get("/schedule", response_model=ScheduleResponse)
async def get_home_schedule(
    store_id: str | None = Query(default=None),
    business_date: str | None = Query(default=None),
    service: HomeService = Depends(get_home_service),
) -> ScheduleResponse:
    return await service.get_schedule(
        DashboardHomeRequest(store_id=store_id, business_date=business_date)
    )
