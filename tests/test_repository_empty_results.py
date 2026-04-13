from __future__ import annotations

import pytest

from app.repositories.ordering_repository import OrderingRepository
from app.repositories.production_repository import ProductionRepository
from app.services.production_service import ProductionService


@pytest.mark.asyncio
async def test_ordering_repository_returns_empty_options_without_engine() -> None:
    repository = OrderingRepository(engine=None)

    options = await repository.list_options()

    assert options == []


@pytest.mark.asyncio
async def test_production_repository_returns_empty_items_without_engine() -> None:
    repository = ProductionRepository(engine=None)

    items = await repository.list_items()

    assert items == []


@pytest.mark.asyncio
async def test_production_service_overview_is_empty_when_repository_is_empty() -> None:
    service = ProductionService(repository=ProductionRepository(engine=None))

    overview = await service.get_overview()

    assert overview.items == []
    assert overview.danger_count == 0
