from __future__ import annotations

from app.repositories.stores_repository import StoresRepository


class StoresService:
    def __init__(self, repository: StoresRepository) -> None:
        self.repository = repository

    async def list_stores(self) -> list[dict]:
        """점포 목록 조회"""
        return await self.repository.list_stores()