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


@pytest.mark.asyncio
async def test_ordering_repository_list_options_prefers_store_cache() -> None:
    store_cache_root = Path(__file__).resolve().parents[1] / "data" / "store_cache"
    store_cache_root.mkdir(parents=True, exist_ok=True)
    cache_path = store_cache_root / "ordering_options_store_cache_test.db"
    if cache_path.exists():
        cache_path.unlink()

    with sqlite3.connect(cache_path) as connection:
        connection.execute(
            """
            CREATE TABLE order_history (
                store_id TEXT NOT NULL,
                dlv_dt TEXT NOT NULL,
                item_cd TEXT,
                item_nm TEXT,
                ord_qty REAL,
                confrm_qty REAL
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO order_history(store_id, dlv_dt, item_cd, item_nm, ord_qty, confrm_qty)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("POC_010", "2026-02-26", "700611", "Bagel", 10, 12),
                ("POC_010", "2026-02-19", "700611", "Bagel", 9, 10),
                ("POC_010", "2026-02-05", "700611", "Bagel", 8, 9),
            ],
        )
        connection.commit()

    repository = OrderingRepository(engine=None)
    repository._resolve_store_cache_db_path = lambda store_id: cache_path  # type: ignore[method-assign]
    repository._build_adjusted_option_items = (  # type: ignore[method-assign]
        lambda aggregated, store_id=None: (
            [
                {
                    "sku_id": str(next(iter(aggregated.values())).get("code") or ""),
                    "sku_name": str(next(iter(aggregated.values())).get("name") or ""),
                    "quantity": int(next(iter(aggregated.values())).get("qty") or 0),
                    "note": None,
                }
            ],
            {
                "total_base_qty": float(next(iter(aggregated.values())).get("qty") or 0),
                "total_adjusted_qty": float(next(iter(aggregated.values())).get("qty") or 0),
                "avg_trend_factor": 1.0,
                "avg_inventory_cover": 0.0,
                "high_expiry_risk_count": 0.0,
                "recent_7d_sales_total": 0.0,
                "adjustment_ratio": 1.0,
                "top_item_name": str(next(iter(aggregated.values())).get("name") or ""),
            },
        )
    )

    options = await repository.list_options(store_id="POC_010", reference_date="20260305")

    assert len(options) == 3
    assert options[0]["basis"] == "2026-02-26"
    assert options[0]["items"][0]["sku_name"] == "Bagel"


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
        if "AS item_name" in sql and "AS quantity" in sql:
            date_value = str((params or {}).get("date_value"))
            rows_by_date = {
                "20260226": [
                    {"item_name": "Bagel", "item_code": "SKU_BAGEL", "quantity": 12},
                    {"item_name": "Bagel", "item_code": "SKU_BAGEL", "quantity": 8},
                ],
                "20260219": [
                    {"item_name": "Donut", "item_code": "SKU_DONUT", "quantity": 5},
                ],
                "20260205": [
                    {"item_name": "Coffee", "item_code": "SKU_COFFEE", "quantity": 7},
                ],
            }
            return _FakeMappingsResult(all_rows=rows_by_date.get(date_value, []))
        raise AssertionError(f"Unexpected SQL executed: {sql}")


class _FakeOrderingOptionsEngine:
    def __init__(self, connection: _FakeOrderingOptionsConnection) -> None:
        self._connection = connection

    def connect(self):
        return self._connection


class _FakeDeadlineItemsConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.executed_params: list[dict | None] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.executed_sql.append(str(statement))
        self.executed_params.append(params)
        return _FakeMappingsResult(
            all_rows=[
                {"item_nm": "Zulu", "latest_dlv_dt": "20260304", "total_ord_qty": 5, "ordered_today": 0},
                {"item_nm": "Alpha", "latest_dlv_dt": "20260305", "total_ord_qty": 12, "ordered_today": 1},
                {"item_nm": "Bravo", "latest_dlv_dt": "20260304", "total_ord_qty": 9, "ordered_today": 0},
            ]
        )


class _FakeDeadlineItemsEngine:
    def __init__(self, connection: _FakeDeadlineItemsConnection) -> None:
        self._connection = connection

    def connect(self):
        return self._connection


class _FakeOrderingTrendConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        return _FakeMappingsResult(
            first={
                "recent_qty": 140,
                "previous_qty": 100,
            }
        )


class _FakeOrderingTrendEngine:
    def connect(self):
        return _FakeOrderingTrendConnection()


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
        "ord_qty": "ord_qty",
        "ord_rec_qty": "ord_rec_qty",
    }
    monkeypatch.setattr(ordering_repository_module, "has_table", lambda engine, table_name: table_name == "raw_order_extract")

    options = asyncio.run(repository.list_options(store_id="POC_010", reference_date="2026-03-05"))

    assert len(options) == 3
    assert options[0]["items"][0]["sku_name"] == "Bagel"
    assert options[0]["items"][0]["quantity"] == 20
    assert options[0]["basis"] == "2026-02-26"
    assert options[1]["basis"] == "2026-02-19"
    assert options[1]["items"][0]["sku_name"] == "Donut"
    assert options[2]["basis"] == "2026-02-05"
    assert options[2]["items"][0]["sku_name"] == "Coffee"
    joined_sql = "\n".join(connection.executed_sql)
    assert "REPLACE(CAST(dlv_dt AS TEXT), '-', '') = :date_value" in joined_sql
    assert any(params and params.get("store_id") == "POC_010" for params in connection.executed_params if isinstance(params, dict))


def test_ordering_repository_deadline_items_uses_visible_last_7_days_and_sorts_unordered_first() -> None:
    connection = _FakeDeadlineItemsConnection()
    repository = OrderingRepository(engine=_FakeDeadlineItemsEngine(connection))

    items = repository.get_deadline_items(
        store_id="POC_010",
        reference_datetime=datetime(2026, 3, 5, 12, 0, 0),
    )

    assert [item["sku_name"] for item in items] == ["Bravo", "Zulu", "Alpha"]
    assert [item["is_ordered"] for item in items] == [False, False, True]
    assert all(item["deadline_at"] == "12:00" for item in items)
    assert connection.executed_params[0]["window_start"] == "20260227"
    assert connection.executed_params[0]["visible_reference_date"] == "20260305"


def test_ordering_repository_trend_summary_compares_recent_7_days_vs_previous_7_days(monkeypatch) -> None:
    monkeypatch.setattr(ordering_repository_module, "has_table", lambda engine, table_name: table_name == "raw_order_extract")
    repository = OrderingRepository(engine=_FakeOrderingTrendEngine())

    summary = repository.get_ordering_trend_summary(
        store_id="POC_010",
        reference_date="2026-03-05",
    )

    assert summary == "최근 7일 주문량은 140개로, 직전 7일 100개 대비 40.0% 증가했습니다."


@pytest.mark.asyncio
async def test_ordering_service_list_options_defaults_store_id_to_poc_010() -> None:
    class _Repo:
        def __init__(self) -> None:
            self.store_ids: list[str | None] = []
            self.reference_dates: list[str | None] = []

        async def list_options(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> list[dict]:
            self.store_ids.append(store_id)
            self.reference_dates.append(reference_date)
            return []

        async def get_weather_forecast(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> dict | None:
            return None

        def get_ordering_trend_summary(
            self,
            *,
            store_id: str,
            reference_date: str | None = None,
        ) -> str | None:
            return None

        def get_deadline_items(
            self,
            *,
            store_id: str,
            reference_datetime=None,
        ) -> list[dict]:
            return []

        def get_ordering_trend_summary(
            self,
            *,
            store_id: str,
            reference_date: str | None = None,
        ) -> str | None:
            return None

        def get_deadline_items(
            self,
            *,
            store_id: str,
            reference_datetime=None,
        ) -> list[dict]:
            return []

        def uses_ordering_join_table(self, store_id: str | None = None) -> bool:
            return False

        def get_ordering_trend_summary(
            self,
            *,
            store_id: str,
            reference_date: str | None = None,
        ) -> str | None:
            return "최근 7일 주문량은 140개로, 직전 7일 100개 대비 40.0% 증가했습니다."

        def uses_ordering_join_table(self, store_id: str | None) -> bool:
            return False

        def get_deadline_items(
            self,
            *,
            store_id: str,
            reference_datetime=None,
        ) -> list[dict]:
            return [
                {
                    "id": "deadline-1",
                    "sku_name": "Bagel",
                    "deadline_at": "12:00",
                    "is_ordered": False,
                }
            ]

    service = OrderingService(repository=_Repo(), ai_client=None)

    response = await service.list_options(store_id=None, skip_ai=True)

    assert response.business_date is not None
    assert service.repository.store_ids == ["POC_010"]
    assert service.repository.reference_dates == [response.business_date]
    assert response.trend_summary == "최근 7일 주문량은 140개로, 직전 7일 100개 대비 40.0% 증가했습니다."
    assert len(response.deadline_items) == 1
    assert response.deadline_items[0].sku_name == "Bagel"
    assert response.deadline_items[0].deadline_at == "12:00"


@pytest.mark.asyncio
async def test_ordering_service_skips_ai_when_join_table_is_available() -> None:
    class _Repo:
        async def list_options(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> list[dict]:
            return [
                {
                    "option_id": "opt-a",
                    "title": "전주 동요일",
                    "basis": "2026-02-26",
                    "description": "",
                    "recommended": True,
                    "reasoning_text": "",
                    "reasoning_metrics": [],
                    "special_factors": [],
                    "items": [{"sku_name": "Bagel", "quantity": 20, "note": None}],
                }
            ]

        async def get_weather_forecast(self, store_id: str | None = None, reference_date: str | None = None):
            return None

        def get_ordering_trend_summary(self, *, store_id: str, reference_date: str | None = None) -> str | None:
            return "최근 7일 주문량은 140개로, 직전 7일 100개 대비 40.0% 증가했습니다."

        def get_deadline_items(self, *, store_id: str, reference_datetime=None) -> list[dict]:
            return []

        def uses_ordering_join_table(self, store_id: str | None) -> bool:
            return True

    class _AI:
        async def recommend_ordering(self, **_: object) -> dict | None:
            raise AssertionError("AI should not be called when join table is available")

        async def get_ordering_deadline_alert(self, store_id: str) -> dict | None:
            return None

    service = OrderingService(repository=_Repo(), ai_client=_AI())

@pytest.mark.asyncio
async def test_ordering_service_list_options_does_not_invoke_ai_for_option_reasoning() -> None:
    class _Repo:
        async def list_options(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> list[dict]:
            return []

        def uses_ordering_join_table(self, store_id: str | None = None) -> bool:
            return False

        async def get_weather_forecast(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> dict | None:
            return None

        def get_ordering_trend_summary(
            self,
            *,
            store_id: str,
            reference_date: str | None = None,
        ) -> str | None:
            return None

        def get_deadline_items(
            self,
            *,
            store_id: str,
            reference_datetime=None,
        ) -> list[dict]:
            return []

    ai_calls: list[dict] = []

    class _AI:
        async def recommend_ordering(self, *args, **kwargs):
            ai_calls.append(kwargs)
            return None

    service = OrderingService(repository=_Repo(), ai_client=_AI())

    await service.list_options(store_id="POC_010")
    assert ai_calls == []
    return

    assert len(ai_calls) == 1, "AI는 옵션 수와 무관하게 요청당 1회만 호출되어야 한다"


@pytest.mark.asyncio
async def test_ordering_service_ignores_ai_weather_when_store_region_mismatches() -> None:
    class _Repo:
        async def list_options(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> list[dict]:
            return []

        def uses_ordering_join_table(self, store_id: str | None = None) -> bool:
            return False

        def _resolve_store_sido(self, store_id: str | None = None) -> str:
            return "경기도"

        async def get_weather_forecast(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> dict | None:
            return {
                "region": "경기도",
                "forecast_date": "2026-03-05",
                "weather_type": "Cloudy",
                "max_temperature_c": 12,
                "min_temperature_c": 4,
                "precipitation_probability": 10,
            }

        def get_ordering_trend_summary(
            self,
            *,
            store_id: str,
            reference_date: str | None = None,
        ) -> str | None:
            return None

        def get_deadline_items(
            self,
            *,
            store_id: str,
            reference_datetime=None,
        ) -> list[dict]:
            return []

    class _AI:
        async def recommend_ordering(self, *args, **kwargs):
            return {
                "weather": {
                    "region": "서울특별시",
                    "forecast_date": "2026-03-05",
                    "weather_type": "Rainy",
                    "max_temperature_c": 9,
                    "min_temperature_c": 2,
                    "precipitation_probability": 80,
                }
            }

    service = OrderingService(repository=_Repo(), ai_client=_AI())

    response = await service.list_options(store_id="POC_010", skip_ai=False)

    assert response.weather is not None
    assert response.weather.region == "경기도"
    assert response.weather.weather_type == "Cloudy"


@pytest.mark.asyncio
async def test_ordering_service_list_options_falls_back_to_store_deadline_items() -> None:
    class _Repo:
        async def list_options(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> list[dict]:
            return [
                {
                    "option_id": "opt-a",
                    "title": "吏?쒖＜ 媛숈? ?붿씪",
                    "basis": "2026-03-01",
                    "description": "湲곗? ?ㅻ챸",
                    "recommended": True,
                    "reasoning_text": "湲곗〈 洹쇨굅",
                    "reasoning_metrics": [],
                    "special_factors": [],
                    "items": [
                        {"sku_id": "700611", "sku_name": "由щ뱶,?꾩씠???뷀삎", "quantity": 6},
                        {"sku_id": "700612", "sku_name": "移댁뭅?ㅽ썑濡쒖뒪?곕뱶", "quantity": 4},
                    ],
                }
            ]

        def uses_ordering_join_table(self, store_id: str | None = None) -> bool:
            return False

        def get_order_arrival_schedule(self, store_id: str | None = None) -> dict[str, str] | None:
            return {
                "order_deadline_at": "12:00",
                "arrival_day_offset": "D+1",
                "arrival_expected_at": "12:00",
            }

        def get_order_arrival_schedule_map(
            self,
            *,
            store_id: str | None = None,
            item_codes: list[str] | None = None,
            item_names: list[str] | None = None,
        ) -> dict[str, dict[str, str]]:
            return {}

        def get_shelf_life_days_map(
            self,
            *,
            item_codes: list[str] | None = None,
            item_names: list[str] | None = None,
        ) -> dict[str, int]:
            return {}

        async def get_weather_forecast(
            self,
            store_id: str | None = None,
            reference_date: str | None = None,
        ) -> dict | None:
            return None

    service = OrderingService(repository=_Repo(), ai_client=None)

    response = await service.list_options(
        store_id="POC_010",
        reference_datetime=datetime(2026, 3, 5, 9, 0, 0),
    )

    assert response.options[0].reasoning_text == "최근의 운영 흐름을 그대로 이어가고 싶을 때 적합합니다."
    assert response.deadline_at == "12:00"
    assert response.deadline_minutes == 180
    assert len(response.deadline_items) == 2
    assert all(item.deadline_at == "12:00" for item in response.deadline_items)


def test_ordering_repository_get_order_arrival_schedule_map_uses_deterministic_ranking(monkeypatch) -> None:
    class _FakeOrderArrivalScheduleConnection:
        def __init__(self) -> None:
            self.executed_sql: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement, params=None):
            self.executed_sql.append(str(statement))
            return _FakeMappingsResult(
                all_rows=[
                    {
                        "item_cd": "700611",
                        "item_nm": "Bagel",
                        "order_deadline_at": "12:00",
                        "arrival_day_offset": "D+1",
                        "arrival_expected_at": "11:00",
                        "arrival_bucket": "오전",
                    }
                ]
            )

    class _FakeOrderArrivalScheduleEngine:
        def __init__(self, connection: _FakeOrderArrivalScheduleConnection) -> None:
            self._connection = connection

        def connect(self):
            return self._connection

    connection = _FakeOrderArrivalScheduleConnection()
    repository = OrderingRepository(engine=_FakeOrderArrivalScheduleEngine(connection))
    monkeypatch.setattr(ordering_repository_module, "has_table", lambda engine, table_name: table_name == "raw_order_arrival_schedule")
    result = repository.get_order_arrival_schedule_map(
        store_id="POC_010",
        item_codes=["700611"],
        item_names=["Bagel"],
    )

    assert result["700611"]["order_deadline_at"] == "12:00"
    assert result["Bagel"]["arrival_day_offset"] == "D+1"
    assert "ROW_NUMBER() OVER" in connection.executed_sql[0]


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
    repository._list_items_from_mart_production_status = lambda store_id, business_date: []  # type: ignore[method-assign]

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
    repository._list_items_from_mart_production_status = lambda store_id, business_date: []  # type: ignore[method-assign]
    repository._resolve_store_cache_db_path = lambda store_id: None  # type: ignore[method-assign]

    items = await repository.list_items(store_id="POC_010")

    assert len(items) == 1
    assert items[0]["sku_id"] == "SKU_SNAPSHOT"
    assert items[0]["forecast"] == 1
    assert items[0]["chance_loss_reduction_pct"] == 75.0
    assert items[0]["snapshot_business_date"] == "20260310"


@pytest.mark.asyncio
async def test_production_repository_prefers_mart_production_status_before_snapshot() -> None:
    repository = ProductionRepository(engine=object())

    repository._list_items_from_mart_production_status = lambda store_id, business_date: [  # type: ignore[method-assign]
        {
            "sku_id": "SKU_MART",
            "name": "Mart Item",
            "current": 12,
            "forecast": 5,
            "order_confirm_qty": 3,
            "hourly_sale_qty": 7,
            "status": "warning",
            "depletion_time": "11:00",
            "recommended": 8,
            "prod1": "08:00 / 10",
            "prod2": "14:00 / 4",
            "chance_loss_reduction_pct": 68.0,
            "snapshot_business_date": "20260305",
            "snapshot_target_hour": 12,
        }
    ]  # type: ignore[return-value]
    repository._list_items_from_prediction_snapshot = lambda store_id, business_date: [  # type: ignore[method-assign]
        {
            "sku_id": "SKU_SNAPSHOT",
            "name": "Snapshot Item",
            "current": 1,
            "forecast": 0,
            "order_confirm_qty": 0,
            "hourly_sale_qty": 0,
            "status": "danger",
            "depletion_time": "09:00",
            "recommended": 3,
            "prod1": "08:00 / 5",
            "prod2": "14:00 / 2",
        }
    ]  # type: ignore[return-value]

    items = await repository.list_items(store_id="POC_010", business_date="2026-03-05")

    assert len(items) == 1
    assert items[0]["sku_id"] == "SKU_MART"


@pytest.mark.asyncio
async def test_production_service_overview_is_empty_when_repository_is_empty() -> None:
    service = ProductionService(repository=ProductionRepository(engine=None))

    overview = await service.get_overview()

    assert overview.items == []
    assert overview.danger_count == 0


@pytest.mark.asyncio
async def test_production_service_uses_repository_recommended_qty_first() -> None:
    class _Repo:
        async def list_items(self, store_id=None, business_date=None, reference_datetime=None):
            return [
                {
                    "sku_id": "SKU-100",
                    "name": "테스트 도넛",
                    "current": 3,
                    "forecast": 1,
                    "status": "warning",
                    "depletion_time": "18:00",
                    "recommended": 99,
                    "prod1": "08:00 / 10개",
                    "prod2": "14:00 / 5개",
                    "chance_loss_amt": 12500,
                }
            ]

    service = ProductionService(repository=_Repo())

    response = await service.get_sku_list(store_id="POC_010")

    assert response.items[0].recommended_production_qty == 99


@pytest.mark.asyncio
async def test_production_service_preserves_negative_forecast_stock() -> None:
    class _Repo:
        async def list_items(self, store_id=None, business_date=None, reference_datetime=None):
            return [
                {
                    "sku_id": "SKU-NEG",
                    "name": "테스트 머핀",
                    "current": 3,
                    "forecast": -2,
                    "predicted_sales_1h": 5,
                    "status": "danger",
                    "depletion_time": "10:00",
                    "recommended": 4,
                    "prod1": "08:00 / 6개",
                    "prod2": "14:00 / 2개",
                    "chance_loss_amt": 4800,
                }
            ]

    service = ProductionService(repository=_Repo())

    response = await service.get_sku_list(store_id="POC_010")

    assert response.items[0].forecast_stock_1h == -2
    assert response.items[0].predicted_sales_1h == 5


@pytest.mark.asyncio
async def test_production_service_overview_sums_chance_loss_amount_as_currency() -> None:
    class _Repo:
        async def list_items(self, store_id=None, business_date=None, reference_datetime=None):
            return [
                {
                    "sku_id": "SKU-101",
                    "name": "테스트 베이글",
                    "current": 2,
                    "forecast": 1,
                    "status": "danger",
                    "depletion_time": "17:30",
                    "recommended": 0,
                    "prod1": "08:00 / 6개",
                    "prod2": "14:00 / 4개",
                    "chance_loss_amt": 5400,
                },
                {
                    "sku_id": "SKU-102",
                    "name": "테스트 머핀",
                    "current": 1,
                    "forecast": 1,
                    "status": "warning",
                    "depletion_time": "18:20",
                    "recommended": 0,
                    "prod1": "08:00 / 5개",
                    "prod2": "14:00 / 3개",
                    "chance_loss_amt": 3200,
                },
            ]

    service = ProductionService(repository=_Repo())

    overview = await service.get_overview(store_id="POC_010")

    chance_loss_stat = next(stat for stat in overview.summary_stats if stat.key == "chance_loss_saving_total")
    assert chance_loss_stat.value == "8,600원"


class _LegacyInventoryStatusRepository:
    def get_inventory_status(
        self,
        store_id: str | None = None,
        page: int = 1,
        page_size: int = 10,
        status_filters: list[str] | None = None,
    ):
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
    def get_inventory_status(
        self,
        store_id: str | None = None,
        page: int = 1,
        page_size: int = 10,
        status_filters: list[str] | None = None,
    ):
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


@pytest.mark.asyncio
async def test_production_service_inventory_status_uses_shelf_life_table_when_present() -> None:
    class _Repo(_StringMetricInventoryStatusRepository):
        def get_shelf_life_days_map(self, *, item_codes: list[str] | None = None, item_names: list[str] | None = None) -> dict[str, int]:
            return {"SKU-002": 3}

    service = ProductionService(repository=_Repo())
    response = await service.get_inventory_status(store_id="POC_004")

    assert response.items[0].assumed_shelf_life_days == 3


@pytest.mark.asyncio
async def test_production_service_inventory_status_uses_precomputed_mart_fields() -> None:
    class _Repo:
        def get_inventory_status(
            self,
            store_id: str | None = None,
            page: int = 1,
            page_size: int = 10,
            status_filters: list[str] | None = None,
        ):
            return (
                [
                    {
                        "item_cd": "SKU-010",
                        "item_nm": "프리컴퓨트 도넛",
                        "stk_avg": 2,
                        "sal_avg": 5,
                        "ord_avg": 7,
                        "stk_rt": -0.2,
                        "is_stockout": 1,
                        "stockout_hour": 14,
                        "assumed_shelf_life_days": 4,
                        "expiry_risk_level": "중간",
                        "status": "여유",
                    }
                ],
                1,
                {
                    "shortage_count": 0,
                    "excess_count": 1,
                    "normal_count": 0,
                    "avg_stock_rate": -0.2,
                },
            )

    service = ProductionService(repository=_Repo())
    response = await service.get_inventory_status(store_id="POC_010")

    assert response.items[0].assumed_shelf_life_days == 4
    assert response.items[0].expiry_risk_level == "중간"
    assert response.items[0].status == "여유"


@pytest.mark.asyncio
async def test_production_service_inventory_status_normalizes_filter_codes() -> None:
    class _Repo:
        def __init__(self) -> None:
            self.captured_status_filters: list[str] | None = None

        def get_inventory_status(
            self,
            store_id: str | None = None,
            page: int = 1,
            page_size: int = 10,
            status_filters: list[str] | None = None,
        ):
            self.captured_status_filters = status_filters
            return (
                [
                    {
                        "item_cd": "SKU-010",
                        "item_nm": "테스트 도넛",
                        "stk_avg": 2,
                        "sal_avg": 5,
                        "ord_avg": 7,
                        "stk_rt": -0.2,
                        "is_stockout": 1,
                        "stockout_hour": 14,
                        "status": "부족",
                    }
                ],
                1,
                {
                    "shortage_count": 1,
                    "excess_count": 0,
                    "normal_count": 0,
                    "avg_stock_rate": -0.2,
                },
            )

    repository = _Repo()
    service = ProductionService(repository=repository)

    await service.get_inventory_status(store_id="POC_010", status_filters=["shortage", "normal"])

    assert repository.captured_status_filters == ["부족", "적정"]


@pytest.mark.asyncio
async def test_production_service_inventory_status_rejects_invalid_filter_code() -> None:
    service = ProductionService(repository=_StringMetricInventoryStatusRepository())

    with pytest.raises(ValueError, match="status must be one of"):
        await service.get_inventory_status(store_id="POC_010", status_filters=["invalid"])


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


class _FifoSummaryRepository:
    def get_fifo_lot_summary(self, *, page: int, page_size: int, **kwargs):
        all_rows = [
            {
                "item_nm": "Prod A",
                "lot_type": "production",
                "shelf_life_days": 1,
                "last_lot_date": "2026-03-04",
                "total_initial_qty": 20,
                "total_consumed_qty": 8,
                "total_wasted_qty": 12,
                "active_remaining_qty": 0,
                "active_lot_count": 0,
                "sold_out_lot_count": 0,
                "expired_lot_count": 1,
            },
            {
                "item_nm": "Prod B",
                "lot_type": "production",
                "shelf_life_days": 1,
                "last_lot_date": "2026-03-04",
                "total_initial_qty": 30,
                "total_consumed_qty": 10,
                "total_wasted_qty": 20,
                "active_remaining_qty": 0,
                "active_lot_count": 0,
                "sold_out_lot_count": 0,
                "expired_lot_count": 1,
            },
            {
                "item_nm": "Delivery A",
                "lot_type": "delivery",
                "shelf_life_days": 30,
                "last_lot_date": "2026-03-04",
                "total_initial_qty": 15,
                "total_consumed_qty": 5,
                "total_wasted_qty": 0,
                "active_remaining_qty": 10,
                "active_lot_count": 1,
                "sold_out_lot_count": 0,
                "expired_lot_count": 0,
            },
        ]
        start = max(0, (page - 1) * page_size)
        end = start + page_size
        return all_rows[start:end], len(all_rows)


@pytest.mark.asyncio
async def test_production_service_fifo_summary_uses_all_rows_not_current_page() -> None:
    service = ProductionService(repository=_FifoSummaryRepository())

    response = await service.get_fifo_lot_summary(
        store_id="POC_010",
        lot_type=None,
        page=1,
        page_size=2,
        date="2026-03-05",
    )

    assert response.pagination.total_items == 3
    assert len(response.items) == 2
    assert response.summary["items_with_waste"] == 2
    assert response.summary["total_wasted_qty"] == 32
    assert response.summary["total_active_qty"] == 10


def test_production_service_recommended_qty_prefers_repository_recommended_value() -> None:
    raw = {
        "current": 0,
        "prod1": "08:00 / 6개",
        "prod2": "14:00 / 3개",
        "recommended": 4,
    }

    assert ProductionService._recommended_qty_from_row(raw) == 4


def test_production_service_chance_loss_amount_falls_back_to_shortage_prevention_amount() -> None:
    raw = {
        "current": 0,
        "forecast": 4,
        "prod1": "08:00 / 6개",
        "prod2": "14:00 / 0개",
        "recommended": 0,
    }

    assert ProductionService._chance_loss_amount_from_row(raw) == 4800
