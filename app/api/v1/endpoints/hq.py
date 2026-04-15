from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.auth import require_roles
from app.core.deps import get_hq_service
from app.schemas.hq import HQCoachingResponse, HQInspectionResponse
from app.services.hq_service import HQService

router = APIRouter(prefix="/hq", tags=["hq"])

_HQ_ROLES = ("hq_admin", "hq_operator")


@router.get(
    "/coaching",
    response_model=HQCoachingResponse,
    dependencies=[Depends(require_roles(*_HQ_ROLES))],
)
async def get_hq_coaching(
    service: HQService = Depends(get_hq_service),
) -> HQCoachingResponse:
    """담당 매장 주문 코칭 데이터를 반환합니다. hq_admin·hq_operator 역할 전용입니다."""
    return await service.get_coaching()


@router.get(
    "/inspection",
    response_model=HQInspectionResponse,
    dependencies=[Depends(require_roles(*_HQ_ROLES))],
)
async def get_hq_inspection(
    service: HQService = Depends(get_hq_service),
) -> HQInspectionResponse:
    """담당 매장 생산 점검 데이터를 반환합니다. hq_admin·hq_operator 역할 전용입니다."""
    return await service.get_inspection()
