from __future__ import annotations

from app.repositories.signals_repository import SignalsRepository
from app.schemas.signals import SalesSignal, SignalsResponse


class SignalsService:
    def __init__(self, repository: SignalsRepository) -> None:
        self.repository = repository

    async def list_signals(self) -> SignalsResponse:
        items = await self.repository.list_signals()
        parsed = [SalesSignal(**item) for item in items]
        return SignalsResponse(items=parsed, high_count=sum(1 for item in parsed if item.priority == "high"))
