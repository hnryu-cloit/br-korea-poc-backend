from fastapi import APIRouter, Depends

from app.core.deps import get_bootstrap_service
from app.schemas.bootstrap import BootstrapResponse
from app.services.bootstrap_service import BootstrapService

router = APIRouter(prefix="/bootstrap", tags=["bootstrap"])


@router.get("", response_model=BootstrapResponse)
async def get_bootstrap(
    service: BootstrapService = Depends(get_bootstrap_service),
) -> BootstrapResponse:
    data = await service.get_bootstrap()
    return BootstrapResponse(**data)
