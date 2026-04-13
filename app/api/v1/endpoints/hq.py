from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.deps import get_hq_service
from app.schemas.hq import HQCoachingResponse, HQInspectionResponse
from app.services.hq_service import HQService

router = APIRouter(prefix="/hq", tags=["hq"])


@router.get("/coaching", response_model=HQCoachingResponse)
async def get_hq_coaching(
    service: HQService = Depends(get_hq_service),
) -> HQCoachingResponse:
    return await service.get_coaching()


@router.get("/inspection", response_model=HQInspectionResponse)
async def get_hq_inspection(
    service: HQService = Depends(get_hq_service),
) -> HQInspectionResponse:
    return await service.get_inspection()
