from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.deps import get_signals_service
from app.schemas.signals import SignalsResponse
from app.services.signals_service import SignalsService

router = APIRouter(prefix="/signals", tags=["signals"])


@router.get("", response_model=SignalsResponse)
async def list_signals(
    service: SignalsService = Depends(get_signals_service),
) -> SignalsResponse:
    return await service.list_signals()
