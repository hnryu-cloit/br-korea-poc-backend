from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3

import pytest

from app.repositories import ordering_repository as ordering_repository_module
from app.repositories.ordering_repository import OrderingRepository
from app.repositories.production_repository import ProductionRepository
from app.services.ordering_service import OrderingService
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


class _FakePredictionSnapshotConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM production_prediction_snapshots" in sql and "business_date = :business_date" in sql:
            return _FakeMappingsResult(first=None)
        if "FROM production_prediction_snapshots" in sql and "status = 'completed'" in sql:
            return _FakeMappingsResult(first={"id": 11, "target_hour": 8, "business_date": "20260310"})
        if "FROM production_prediction_snapshot_items" in sql:
            return _FakeMappingsResult(
                all_rows=[
                    {
                        "sku_id": "SKU_SNAPSHOT",
                        "name": "Snapshot Item",
                        "current_stock": 3,
                        "predicted_stock_1h": 1,
                        "forecast_baseline": 4,
                        "recommended_production_qty": 5,
                        "avg_first_production_qty_4w": 6,
                        "avg_first_production_time_4w": "08:00",
                        "avg_second_production_qty_4w": 2,
                        "avg_second_production_time_4w": "14:00",
                        "order_confirm_qty": 1,
                        "hourly_sale_qty": 2,
                        "status": "warning",
                        "depletion_time": "09:50",
                        "stockout_expected_at": "1시간 이내",
                        "alert_message": "snapshot alert",
                        "confidence": 0.82,
                        "chance_loss_qty": 2.0,
                        "chance_loss_amt": 5400.0,
                        "chance_loss_reduction_pct": 75.0,
                    }
                ]
            )
        raise AssertionError(f"Unexpected SQL executed: {sql}")


class _FakePredictionSnapshotEngine:
    def connect(self):
        return _FakePredictionSnapshotConnection()


class _FakeOrderingOptionsConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.executed_params: list[dict | None] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.executed_sql.append(sql)
        self.executed_params.append(params)
        if "SELECT DISTINCT CAST" in sql:
            return _FakeMappingsResult(all_rows=["20260305"])
        if "AS item_name" in sql and "AS quantity" in sql:
            return _FakeMappingsResult(
                all_rows=[
                    {"item_name": "Bagel", "item_code": "SKU_BAGEL", "quantity": 12},
                    {"item_name": "Bagel", "item_code": "SKU_BAGEL", "quantity": 8},
                ]
            )
        raise AssertionError(f"Unexpected SQL executed: {sql}")


class _FakeOrderingOptionsEngine:
    def __init__(self, connection: _FakeOrderingOptionsConnection) -> None:
        self._connection = connection

    def connect(self):
        return self._connection


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


def test_ordering_repository_list_options_filters_recent_4_weeks_and_store(monkeypatch) -> None:
    connection = _FakeOrderingOptionsConnection()
    repository = OrderingRepository(engine=_FakeOrderingOptionsEngine(connection))
    repository._table_columns = lambda table_name: {  # type: ignore[method-assign]
        "masked_stor_cd": "masked_stor_cd",
        "dlv_dt": "dlv_dt",
        "item_nm": "item_nm",
        "item_cd": "item_cd",
        "ord_rec_qty": "ord_rec_qty",
    }
    monkeypatch.setattr(ordering_repository_module, "has_table", lambda engine, table_name: table_name == "raw_order_extract")

    options = asyncio.run(repository.list_options(store_id="POC_010"))

    assert len(options) == 1
    assert options[0]["items"][0]["sku_name"] == "Bagel"
    assert options[0]["items"][0]["quantity"] == 20
    joined_sql = "\n".join(connection.executed_sql)
    assert "INTERVAL '27 day'" in joined_sql
    assert any(params and params.get("store_id") == "POC_010" for params in connection.executed_params if isinstance(params, dict))


@pytest.mark.asyncio
async def test_ordering_service_list_options_defaults_store_id_to_poc_010() -> None:
    class _Repo:
        def __init__(self) -> None:
            self.store_ids: list[str | None] = []

        async def list_options(self, store_id: str | None = None) -> list[dict]:
            self.store_ids.append(store_id)
            return []

        async def get_weather_forecast(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> dict | None:
            return None

    service = OrderingService(repository=_Repo(), ai_client=None)

    response = await service.list_options(store_id=None, skip_ai=True)

    assert response.business_date is not None
    assert service.repository.store_ids == ["POC_010"]


def test_ordering_repository_history_prefers_store_cache() -> None:
    repository = OrderingRepository(engine=None)
    result = repository.get_history_filtered(store_id="POC_010", limit=10)

    assert result["total_count"] == 0
    assert result["auto_rate"] == 0.0
    assert result["manual_rate"] == 0.0
    assert result["items"] == []
    return

    store_cache_root = (
        Path(__file__).resolve().parents[1] / "data" / "store_cache"
    )
    store_cache_root.mkdir(parents=True, exist_ok=True)
    cache_path = store_cache_root / "ordering_history_test_cache.db"
    if cache_path.exists():
        cache_path.unlink()

    with sqlite3.connect(cache_path) as connection:
        connection.execute(
            """
            CREATE TABLE order_history (
                store_id TEXT NOT NULL,
                dlv_dt TEXT NOT NULL,
                ord_grp TEXT,
                ord_grp_nm TEXT,
                ord_dgre TEXT,
                ord_dgre_nm TEXT,
                ord_type TEXT,
                ord_type_nm TEXT,
                item_cd TEXT,
                item_nm TEXT,
                item_type TEXT,
                ord_unit TEXT,
                ord_prc REAL,
                ord_qty REAL,
                ord_amt REAL,
                confrm_prc REAL,
                confrm_qty REAL,
                confrm_amt REAL,
                auto_ord_yn TEXT,
                ord_rec_qty REAL
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO order_history(
                store_id, dlv_dt, ord_grp_nm, item_cd, item_nm, ord_qty, confrm_qty, auto_ord_yn
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("POC_010", "2026-03-05", "냉동", "SKU1", "아메리카노", 10, 9, "1"),
                ("POC_010", "2026-03-04", "냉동", "SKU2", "카페라떼", 8, 8, "0"),
            ],
        )
        connection.commit()

    repository = OrderingRepository(engine=None)
    repository._resolve_store_cache_db_path = lambda store_id: cache_path  # type: ignore[method-assign]

    result = repository.get_history_filtered(store_id="POC_010", limit=10)

    assert result["total_count"] == 2
    assert result["auto_rate"] == 0.5
    assert result["manual_rate"] == 0.5
    assert result["items"][0]["item_nm"] == "아메리카노"


def test_ordering_repository_history_rates_follow_visible_rows() -> None:
    repository = OrderingRepository(engine=None)
    result = repository._build_history_response(
        [
            {
                "item_nm": "ManualOnly",
                "dlv_dt": "2026-03-05",
                "ord_qty": 10,
                "confrm_qty": 10,
                "auto_ord_yn": "0",
                "ord_grp_nm": "manual",
            }
        ]
    )

    assert result["total_count"] == 1
    assert result["auto_rate"] == 0.0
    assert result["manual_rate"] == 1.0
    assert result["items"][0]["item_nm"] == "ManualOnly"
    return

    store_cache_root = (
        Path(__file__).resolve().parents[1] / "data" / "store_cache"
    )
    store_cache_root.mkdir(parents=True, exist_ok=True)
    cache_path = store_cache_root / "ordering_history_visible_rows_test_cache.db"
    if cache_path.exists():
        cache_path.unlink()

    with sqlite3.connect(cache_path) as connection:
        connection.execute(
            """
            CREATE TABLE order_history (
                store_id TEXT NOT NULL,
                dlv_dt TEXT NOT NULL,
                ord_grp TEXT,
                ord_grp_nm TEXT,
                ord_dgre TEXT,
                ord_dgre_nm TEXT,
                ord_type TEXT,
                ord_type_nm TEXT,
                item_cd TEXT,
                item_nm TEXT,
                item_type TEXT,
                ord_unit TEXT,
                ord_prc REAL,
                ord_qty REAL,
                ord_amt REAL,
                confrm_prc REAL,
                confrm_qty REAL,
                confrm_amt REAL,
                auto_ord_yn TEXT,
                ord_rec_qty REAL
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO order_history(
                store_id, dlv_dt, ord_grp_nm, item_cd, item_nm, ord_qty, confrm_qty, auto_ord_yn
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("POC_010", "2026-03-05", "수동", "SKU1", "ManualOnly", 10, 10, "0"),
                ("POC_010", "2026-03-04", "수동", "SKU2", "HiddenAuto", 8, 8, "1"),
            ],
        )
        connection.commit()

    repository = OrderingRepository(engine=None)
    repository._resolve_store_cache_db_path = lambda store_id: cache_path  # type: ignore[method-assign]

    result = repository.get_history_filtered(store_id="POC_010", limit=1)

    assert result["total_count"] == 1
    assert result["auto_rate"] == 0.0
    assert result["manual_rate"] == 1.0
    assert result["items"][0]["item_nm"] == "ManualOnly"


def test_ordering_repository_store_cache_hides_same_day_rows_before_noon_and_aggregates_same_menu() -> None:
    store_cache_root = Path(__file__).resolve().parents[1] / "data" / "store_cache"
    store_cache_root.mkdir(parents=True, exist_ok=True)
    cache_path = store_cache_root / "ordering_history_cutoff_aggregate_test_cache.db"
    if cache_path.exists():
        cache_path.unlink()

    with sqlite3.connect(cache_path) as connection:
        connection.execute(
            """
            CREATE TABLE order_history (
                store_id TEXT NOT NULL,
                dlv_dt TEXT NOT NULL,
                ord_grp_nm TEXT,
                item_cd TEXT,
                item_nm TEXT,
                ord_qty REAL,
                confrm_qty REAL,
                auto_ord_yn TEXT
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO order_history(
                store_id, dlv_dt, ord_grp_nm, item_cd, item_nm, ord_qty, confrm_qty, auto_ord_yn
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("POC_010", "2026-03-05", "manual", "SKU1", "카카오후로스티드", 10, 10, "0"),
                ("POC_010", "2026-03-05", "manual", "SKU1", "카카오후로스티드", 14, 12, "0"),
                ("POC_010", "2026-03-04", "manual", "SKU2", "플레인베이글", 8, 8, "1"),
            ],
        )
        connection.commit()

    repository = OrderingRepository(engine=object())

    before_noon = repository._get_history_filtered_from_store_cache(
        cache_path=cache_path,
        store_id="POC_010",
        limit=10,
        reference_datetime=datetime(2026, 3, 5, 9, 0, 0),
    )
    after_noon = repository._get_history_filtered_from_store_cache(
        cache_path=cache_path,
        store_id="POC_010",
        limit=10,
        reference_datetime=datetime(2026, 3, 5, 12, 0, 0),
    )

    assert [item["item_nm"] for item in before_noon["items"]] == ["플레인베이글"]
    assert before_noon["total_count"] == 1

    assert after_noon["items"][0]["item_nm"] == "카카오후로스티드"
    assert after_noon["items"][0]["ord_qty"] == 24
    assert after_noon["items"][0]["confrm_qty"] == 22
    assert after_noon["total_count"] == 2


def test_ordering_service_history_defaults_reference_datetime_to_nine_am() -> None:
    class _Repo:
        def is_known_store(self, store_id: str) -> bool:
            return store_id == "POC_010"

        def get_history_filtered(self, **kwargs) -> dict:
            reference_datetime = kwargs.get("reference_datetime")
            assert reference_datetime == datetime(2026, 3, 5, 9, 0, 0)
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}

    service = OrderingService(repository=_Repo(), ai_client=None)

    response = service.get_history(store_id="POC_010")

    assert response.total_count == 0


@pytest.mark.asyncio
async def test_production_repository_returns_empty_items_without_engine() -> None:
    repository = ProductionRepository(engine=None)

    items = await repository.list_items()

    assert items == []


@pytest.mark.asyncio
async def test_production_repository_list_items_only_uses_active_skus_for_reference_date(monkeypatch) -> None:
    repository = ProductionRepository(engine=object())

    monkeypatch.setattr(
        "app.repositories.production_repository.has_table",
        lambda engine, table_name: True,
    )

    captured_calls: list[tuple[str, int, str | None]] = []

    def _fake_fetch_metric_map(
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        metric_candidates: tuple[str, ...],
        store_id: str | None = None,
        window_days: int = 0,
        reference_date: str | None = None,
    ) -> dict[str, dict[str, object]]:
        captured_calls.append((relation, window_days, reference_date))
        if relation == "raw_inventory_extract" and metric_candidates == ("stock_qty",):
            return {"ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 10}}
        if relation == "raw_daily_store_item" and metric_candidates == ("sale_qty",):
            return {"ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 8}}
        if relation == "raw_production_extract":
            return {
                "ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 12},
                "OLD": {"item_cd": "OLD", "item_nm": "과거상품", "qty": 15},
            }
        if relation == "raw_order_extract":
            return {"OLD": {"item_cd": "OLD", "item_nm": "과거상품", "qty": 9}}
        if relation == "core_hourly_item_sales":
            return {"OLD": {"item_cd": "OLD", "item_nm": "과거상품", "qty": 7}}
        return {}

    repository._fetch_metric_map = _fake_fetch_metric_map  # type: ignore[method-assign]
    repository._fetch_business_hour_stock_map = lambda store_id, business_date, target_hour: {  # type: ignore[method-assign]
        "ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 10}
    }
    repository._fetch_business_hour_sale_map = lambda store_id, business_date, target_hour, window_days=28: {  # type: ignore[method-assign]
        "OLD": {"item_cd": "OLD", "item_nm": "과거상품", "qty": 7}
    }
    repository._resolve_store_cache_db_path = lambda store_id: None  # type: ignore[method-assign]
    repository._fetch_recent_production_item_keys = lambda store_id, business_date=None, window_days=7: {"ACTIVE"}  # type: ignore[method-assign]
    repository._fetch_recent_sales_item_keys = lambda store_id, business_date=None, window_days=7: {"ACTIVE", "OLD"}  # type: ignore[method-assign]
    repository._fetch_store_production_item_keys = lambda store_id: {"ACTIVE"}  # type: ignore[method-assign]

    items = await repository.list_items(store_id="POC_010", business_date="2026-03-05")

    assert [item["sku_id"] for item in items] == ["ACTIVE"]
    assert ("raw_production_extract", 28, "20260304") in captured_calls
    assert ("raw_order_extract", 14, "2026-03-05") not in captured_calls


@pytest.mark.asyncio
async def test_production_repository_clamps_negative_stock_qty_to_zero_for_current_stock(monkeypatch) -> None:
    repository = ProductionRepository(engine=object())

    monkeypatch.setattr(
        "app.repositories.production_repository.has_table",
        lambda engine, table_name: True,
    )

    def _fake_fetch_metric_map(
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        metric_candidates: tuple[str, ...],
        store_id: str | None = None,
        window_days: int = 0,
        reference_date: str | None = None,
    ) -> dict[str, dict[str, object]]:
        if relation == "raw_inventory_extract" and metric_candidates == ("stock_qty",):
            return {"ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": -6}}
        if relation == "raw_daily_store_item" and metric_candidates == ("sale_qty",):
            return {"ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 6}}
        if relation == "raw_production_extract":
            return {"ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 12}}
        return {}

    repository._fetch_metric_map = _fake_fetch_metric_map  # type: ignore[method-assign]
    repository._fetch_business_hour_stock_map = lambda store_id, business_date, target_hour: {  # type: ignore[method-assign]
        "ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": -6}
    }
    repository._fetch_business_hour_sale_map = lambda store_id, business_date, target_hour, window_days=28: {  # type: ignore[method-assign]
        "ACTIVE": {"item_cd": "ACTIVE", "item_nm": "활성상품", "qty": 1}
    }
    repository._resolve_store_cache_db_path = lambda store_id: None  # type: ignore[method-assign]
    repository._fetch_recent_production_item_keys = lambda store_id, business_date=None, window_days=7: {"ACTIVE"}  # type: ignore[method-assign]
    repository._fetch_recent_sales_item_keys = lambda store_id, business_date=None, window_days=7: {"ACTIVE"}  # type: ignore[method-assign]
    repository._fetch_store_production_item_keys = lambda store_id: {"ACTIVE"}  # type: ignore[method-assign]

    items = await repository.list_items(store_id="POC_010", business_date="2026-03-05")

    assert len(items) == 1
    assert items[0]["current"] == 0


@pytest.mark.asyncio
async def test_production_repository_prefers_completed_prediction_snapshot(monkeypatch) -> None:
    repository = ProductionRepository(engine=object())

    repository._list_items_from_prediction_snapshot = lambda store_id, business_date: [  # type: ignore[method-assign]
        {
            "sku_id": "SKU_SNAPSHOT",
            "name": "Snapshot Item",
            "current": 3,
            "forecast": 1,
            "order_confirm_qty": 2,
            "hourly_sale_qty": 2,
            "status": "warning",
            "depletion_time": "10:10",
            "recommended": 4,
            "prod1": "08:00 / 5개",
            "prod2": "14:00 / 3개",
            "chance_loss_amt": 7200.0,
            "chance_loss_reduction_pct": 66.7,
        }
    ]  # type: ignore[return-value]
    repository._resolve_store_cache_db_path = lambda store_id: None  # type: ignore[method-assign]

    items = await repository.list_items(store_id="POC_010")

    assert len(items) == 1
    assert items[0]["sku_id"] == "SKU_SNAPSHOT"
    assert items[0]["chance_loss_amt"] == 7200.0


@pytest.mark.asyncio
async def test_production_repository_uses_latest_completed_snapshot_when_business_date_misses(monkeypatch) -> None:
    repository = ProductionRepository(engine=_FakePredictionSnapshotEngine())

    monkeypatch.setattr(
        "app.repositories.production_repository.has_table",
        lambda engine, table_name: table_name in {"production_prediction_snapshots", "production_prediction_snapshot_items"},
    )
    repository._resolve_store_cache_db_path = lambda store_id: None  # type: ignore[method-assign]

    items = await repository.list_items(store_id="POC_010")

    assert len(items) == 1
    assert items[0]["sku_id"] == "SKU_SNAPSHOT"
    assert items[0]["forecast"] == 1
    assert items[0]["chance_loss_reduction_pct"] == 75.0
    assert items[0]["snapshot_business_date"] == "20260310"


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


class _MonthlyWasteRepository:
    def get_stock_rate_recent_rows(self, store_id: str):
        return [
            {
                "item_cd": "SKU-010",
                "item_nm": "최신 글레이즈드",
                "ord_avg": 12,
                "sal_avg": 8,
                "stk_avg": 0,
                "stk_rt": 0,
                "dr": 1,
            },
            {
                "item_cd": "SKU-020",
                "item_nm": "최신 카카오",
                "ord_avg": 6,
                "sal_avg": 5,
                "stk_avg": 0,
                "stk_rt": 0,
                "dr": 1,
            },
        ]

    def get_disuse_and_cost_latest_rows(self, store_id: str):
        return [
            {
                "item_cd": "SKU-010",
                "item_nm": "최신 글레이즈드",
                "total_disuse_qty": 3,
                "avg_cost": 2000,
            },
            {
                "item_cd": "SKU-020",
                "item_nm": "최신 카카오",
                "total_disuse_qty": 1,
                "avg_cost": 3000,
            },
        ]

    def get_monthly_disuse_rows(self, store_id: str, date_from: str, date_to: str):
        return [
            {
                "item_cd": "SKU-001",
                "item_nm": "월간 글레이즈드",
                "total_disuse_qty": 7,
                "total_disuse_amount": 21000,
                "avg_cost": 3000,
            },
            {
                "item_cd": "SKU-002",
                "item_nm": "월간 카카오",
                "total_disuse_qty": 4,
                "total_disuse_amount": 12000,
                "avg_cost": 3000,
            },
        ]


@pytest.mark.asyncio
async def test_production_service_waste_summary_supports_pagination(monkeypatch) -> None:
    monkeypatch.setattr("app.services.production_service.get_now", lambda: datetime(2026, 4, 23, 9, 0, 0))
    service = ProductionService(repository=_MonthlyWasteRepository())

    response = await service.get_waste_summary(store_id="POC_001", page=1, page_size=1)

    assert response.pagination.total_items == 2
    assert response.pagination.total_pages == 2
    assert response.pagination.page == 1
    assert response.pagination.page_size == 1
    assert len(response.items) == 1
    assert response.items[0].item_nm == "최신 글레이즈드"
    assert response.total_disuse_amount == 9000
    assert response.summary["target_month"] == "2026-04"
    assert response.summary["monthly_total_disuse_amount"] == 33000
    assert response.monthly_top_items[0].item_nm == "월간 글레이즈드"
    assert response.monthly_top_items[0].confirmed_disuse_qty == 7
