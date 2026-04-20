from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.deps import get_stores_repository
from app.repositories.stores_repository import StoresRepository

router = APIRouter(prefix="/stores", tags=["stores"])


@router.get("")
async def list_stores(
    repository: StoresRepository = Depends(get_stores_repository),
) -> dict:
    """점포 목록을 반환합니다."""
    stores = await repository.list_stores()
    return {"stores": stores}