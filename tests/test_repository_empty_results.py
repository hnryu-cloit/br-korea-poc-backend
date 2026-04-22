from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from app.repositories import ordering_repository as ordering_repository_module
from app.repositories.ordering_repository import OrderingRepository
from app.repositories.production_repository import ProductionRepository
from app.services.production_service import ProductionService


@pytest.mark.asyncio
async def test_ordering_repository_returns_empty_options_without_engine() -> None:
    repository = OrderingRepository(engine=None)

    options = await repository.list_options()

    assert options == []


class _FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value


class _FakeMappingsResult:
    def __init__(self, one=None, first=None, all_rows=None):
        self._one = one
        self._first = first
        self._all_rows = all_rows or []

    def mappings(self):
        return self

    def scalars(self):
        return self

    def one(self):
        return self._one

    def first(self):
        return self._first

    def all(self):
        return self._all_rows


class _FakeOrderingSummaryConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        if "SELECT COUNT(*)" in sql and "FROM ordering_selections" in sql:
            return _FakeScalarResult(0)
        if "LIMIT 1" in sql and "FROM ordering_selections" in sql:
            return _FakeMappingsResult(first=None)
        if "SELECT actor" in sql:
            return _FakeMappingsResult(all_rows=[])
        if "GROUP BY option_id" in sql:
            return _FakeMappingsResult(all_rows=[])
        raise AssertionError(f"Unexpected SQL executed: {sql}")


class _FakeOrderingSummaryEngine:
    def connect(self):
        return _FakeOrderingSummaryConnection()


def test_ordering_selection_summary_uses_last_7_days_window_when_date_from_missing(monkeypatch) -> None:
    monkeypatch.setattr(ordering_repository_module, "has_table", lambda engine, table_name: True)
    repository = OrderingRepository(engine=_FakeOrderingSummaryEngine())
    captured_calls: list[tuple[str | None, str | None, str | None]] = []

    def _record_filters(
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> tuple[str, dict]:
        captured_calls.append((store_id, date_from, date_to))
        return "", {}

    repository._build_history_filters = _record_filters  # type: ignore[method-assign]

    summary = asyncio.run(repository.get_selection_summary(store_id="POC_001"))

    assert summary["recent_selection_count_7d"] == 0
    assert len(captured_calls) == 2
    assert captured_calls[0] == ("POC_001", None, None)
    expected_recent_from = (datetime.now().date() - timedelta(days=6)).isoformat()
    assert captured_calls[1][0] == "POC_001"
    assert captured_calls[1][1] == expected_recent_from
    assert captured_calls[1][2] is None


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
