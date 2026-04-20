from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.deps import get_stores_service
from app.services.stores_service import StoresService

router = APIRouter(prefix="/stores", tags=["stores"])


@router.get("")
async def list_stores(
    service: StoresService = Depends(get_stores_service),
) -> dict:
    """점포 목록을 반환합니다."""
    stores = await service.list_stores()
    return {"stores": stores}