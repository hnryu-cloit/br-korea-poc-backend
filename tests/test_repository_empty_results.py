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


class _LegacyInventoryStatusRepository:
    def get_inventory_status(self, store_id: str | None = None, page: int = 1, page_size: int = 10):
        return (
            [
                {
                    "item_cd": "SKU-001",
                    "item_nm": "테스트 도넛",
                    "stk_avg": 10,
                    "sal_avg": 8,
                    "ord_avg": 10,
                    "stk_rt": 0.1,
                    "is_stockout": 0,
                    "stockout_hour": None,
                }
            ],
            1,
        )


@pytest.mark.asyncio
async def test_production_service_inventory_status_handles_legacy_two_tuple_result() -> None:
    service = ProductionService(repository=_LegacyInventoryStatusRepository())

    response = await service.get_inventory_status(store_id="POC_001")

    assert response.pagination.total_items == 1
    assert len(response.items) == 1
    assert response.items[0].item_cd == "SKU-001"


class _StringMetricInventoryStatusRepository:
    def get_inventory_status(self, store_id: str | None = None, page: int = 1, page_size: int = 10):
        return (
            [
                {
                    "item_cd": "SKU-002",
                    "item_nm": "테스트 베이글",
                    "stk_avg": 4,
                    "sal_avg": 5,
                    "ord_avg": 8,
                    "stk_rt": -0.1,
                    "is_stockout": "0",
                    "stockout_hour": "",
                }
            ],
            1,
            {
                "shortage_count": "1.0",
                "excess_count": "",
                "normal_count": None,
                "avg_stock_rate": "-0.1",
            },
        )


@pytest.mark.asyncio
async def test_production_service_inventory_status_handles_string_summary_metrics() -> None:
    service = ProductionService(repository=_StringMetricInventoryStatusRepository())

    response = await service.get_inventory_status(store_id="POC_003")

    assert response.summary["shortage_count"] == 1
    assert response.summary["excess_count"] == 0
    assert response.summary["normal_count"] == 0
