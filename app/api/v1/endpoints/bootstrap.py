from fastapi import APIRouter

from app.schemas.bootstrap import BootstrapResponse
from app.services.planning_service import PlanningService

router = APIRouter(prefix="/bootstrap", tags=["bootstrap"])
service = PlanningService()


@router.get("", response_model=BootstrapResponse)
async def get_bootstrap() -> BootstrapResponse:
    data = await service.get_bootstrap()
    return BootstrapResponse(**data)