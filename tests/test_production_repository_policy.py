from __future__ import annotations

import re

from app.repositories.production_repository import ProductionRepository


def test_scale_down_poc_qty_boundaries() -> None:
    repository = ProductionRepository(engine=None)

    assert repository._scale_down_poc_qty(30) == 30
    assert repository._scale_down_poc_qty(40) == 30
    assert repository._scale_down_poc_qty(101) == 26
    assert repository._scale_down_poc_qty(501) == 31


def test_build_new_items_marks_danger_and_recommends_qty() -> None:
    repository = ProductionRepository(engine=None)

    items = repository._build_new_items(
        production_map={},
        secondary_map={},
        stock_map={"SKU_A": {"item_cd": "SKU_A", "item_nm": "아메리카노", "qty": 5}},
        sale_map={"SKU_A": {"qty": 40}},
        order_confirm_map={},
        hourly_sale_map={},
    )

    assert len(items) == 1
    item = items[0]

    assert item["sku_id"] == "SKU_A"
    assert item["name"] == "아메리카노"
    assert item["status"] == "danger"
    assert item["recommended"] > 0
    assert item["depletion_time"] != "-"
    assert re.match(r"^\d{2}:\d{2}$", str(item["depletion_time"]))


def test_build_new_items_marks_warning_when_velocity_pressure_exists() -> None:
    repository = ProductionRepository(engine=None)

    items = repository._build_new_items(
        production_map={},
        secondary_map={},
        stock_map={"SKU_B": {"item_cd": "SKU_B", "item_nm": "카페라떼", "qty": 40}},
        sale_map={},
        order_confirm_map={},
        hourly_sale_map={"SKU_B": {"qty": 20}},
    )

    assert len(items) == 1
    item = items[0]

    assert item["status"] == "warning"
    assert item["velocity_pressure"] is True
    assert item["recommended"] > 0


def test_build_new_items_returns_safe_and_no_recommendation_when_forecast_zero() -> None:
    repository = ProductionRepository(engine=None)

    items = repository._build_new_items(
        production_map={},
        secondary_map={},
        stock_map={"SKU_C": {"item_cd": "SKU_C", "item_nm": "글레이즈드", "qty": 10}},
        sale_map={},
        order_confirm_map={},
        hourly_sale_map={},
    )

    assert len(items) == 1
    item = items[0]

    assert item["status"] == "safe"
    assert item["recommended"] == 0
    assert item["depletion_time"] == "-"


def test_build_new_items_keeps_zero_current_when_inventory_row_exists_but_stock_is_negative() -> None:
    repository = ProductionRepository(engine=None)

    items = repository._build_new_items(
        production_map={"SKU_NEG": {"item_cd": "SKU_NEG", "item_nm": "Neg", "qty": 6}},
        secondary_map={},
        stock_map={"SKU_NEG": {"item_cd": "SKU_NEG", "item_nm": "Neg", "qty": -4}},
        sale_map={"SKU_NEG": {"qty": 4}},
        order_confirm_map={},
        hourly_sale_map={},
    )

    assert len(items) == 1
    item = items[0]

    assert item["current"] == 0
    assert item["prod1"] == "08:00 / 6개"


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _MappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _MetricMapConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        if "BETWEEN :min_date AND :max_date" in sql:
            return _MappingsResult(
                [
                    {
                        "item_name": "Blueberry",
                        "item_code": "SKU_B",
                        "date_val": "20260301",
                        "metric_value": 6,
                    },
                    {
                        "item_name": "Blueberry",
                        "item_code": "SKU_B",
                        "date_val": "20260303",
                        "metric_value": 6,
                    },
                ]
            )
        if "ORDER BY date_value DESC" in sql:
            return _ScalarResult("20260306")
        raise AssertionError(f"Unexpected SQL executed: {sql}")


class _MetricMapEngine:
    def connect(self):
        return _MetricMapConnection()


def test_fetch_metric_map_window_average_uses_full_window_days() -> None:
    repository = ProductionRepository(engine=_MetricMapEngine())
    repository._table_columns = lambda _table_name: {  # type: ignore[method-assign]
        "prod_dt": "prod_dt",
        "item_nm": "item_nm",
        "item_cd": "item_cd",
        "prod_qty": "prod_qty",
        "masked_stor_cd": "masked_stor_cd",
    }

    metric_map = repository._fetch_metric_map(
        "raw_production_extract",
        ("prod_dt",),
        ("item_nm",),
        ("item_cd",),
        ("prod_qty",),
        store_id="POC_010",
        window_days=28,
        reference_date="2026-03-06",
    )

    assert metric_map["SKU_B"]["qty"] == 0


def test_infer_stockout_from_hourly_sales_marks_only_terminal_zero_sales_window() -> None:
    repository = ProductionRepository(engine=None)

    stockout_map = repository._infer_stockout_from_hourly_sales(
        inventory_rows=[{"item_cd": "SKU_A", "item_nm": "Glazed"}],
        hourly_rows=[
            {"item_cd": "SKU_A", "item_nm": "Glazed", "tmzon_div": "08", "sale_qty": 2},
            {"item_cd": "SKU_A", "item_nm": "Glazed", "tmzon_div": "09", "sale_qty": 1},
            {"item_cd": "SKU_A", "item_nm": "Glazed", "tmzon_div": "13", "sale_qty": 1},
        ],
    )

    assert stockout_map["SKU_A"]["is_stockout"] is True
    assert stockout_map["SKU_A"]["stockout_hour"] == 14


def test_infer_stockout_from_hourly_sales_does_not_mark_items_without_prior_sale() -> None:
    repository = ProductionRepository(engine=None)

    stockout_map = repository._infer_stockout_from_hourly_sales(
        inventory_rows=[{"item_cd": "SKU_B", "item_nm": "Boston"}],
        hourly_rows=[],
    )

    assert stockout_map["SKU_B"]["is_stockout"] is False
    assert stockout_map["SKU_B"]["stockout_hour"] is None


def test_infer_stockout_from_hourly_sales_ignores_after_close_zero_sales_window() -> None:
    repository = ProductionRepository(engine=None)

    stockout_map = repository._infer_stockout_from_hourly_sales(
        inventory_rows=[{"item_cd": "SKU_C", "item_nm": "Choco"}],
        hourly_rows=[
            {"item_cd": "SKU_C", "item_nm": "Choco", "tmzon_div": "17", "sale_qty": 3},
            {"item_cd": "SKU_C", "item_nm": "Choco", "tmzon_div": "18", "sale_qty": 1},
        ],
        operating_hours={"open_hour": 9, "close_hour": 18},
    )

    assert stockout_map["SKU_C"]["is_stockout"] is False
    assert stockout_map["SKU_C"]["stockout_hour"] is None


def test_infer_stockout_from_hourly_sales_does_not_mark_gap_if_later_sales_resume() -> None:
    repository = ProductionRepository(engine=None)

    stockout_map = repository._infer_stockout_from_hourly_sales(
        inventory_rows=[{"item_cd": "SKU_RESUME", "item_nm": "Resume"}],
        hourly_rows=[
            {"item_cd": "SKU_RESUME", "item_nm": "Resume", "tmzon_div": "08", "sale_qty": 2},
            {"item_cd": "SKU_RESUME", "item_nm": "Resume", "tmzon_div": "15", "sale_qty": 1},
            {"item_cd": "SKU_RESUME", "item_nm": "Resume", "tmzon_div": "20", "sale_qty": 1},
        ],
    )

    assert stockout_map["SKU_RESUME"]["is_stockout"] is False
    assert stockout_map["SKU_RESUME"]["stockout_hour"] is None


def test_infer_stockout_from_hourly_sales_uses_fixed_window_when_operating_hours_invalid() -> None:
    repository = ProductionRepository(engine=None)

    stockout_map = repository._infer_stockout_from_hourly_sales(
        inventory_rows=[{"item_cd": "SKU_D", "item_nm": "Field"}],
        hourly_rows=[
            {"item_cd": "SKU_D", "item_nm": "Field", "tmzon_div": "17", "sale_qty": 2},
            {"item_cd": "SKU_D", "item_nm": "Field", "tmzon_div": "18", "sale_qty": 1},
        ],
        operating_hours={"open_hour": 21, "close_hour": 18},
    )

    assert stockout_map["SKU_D"]["is_stockout"] is True
    assert stockout_map["SKU_D"]["stockout_hour"] == 19


def test_list_inferred_stockout_events_prefers_recorded_stockout_rows() -> None:
    repository = ProductionRepository(engine=None)

    repository._fetch_store_operating_hours = lambda store_id: {"open_hour": 9, "close_hour": 18}  # type: ignore[method-assign]
    repository._fetch_recorded_stockout_rows_for_date = lambda store_id, sale_date: [  # type: ignore[method-assign]
        {"item_cd": "SKU_A", "item_nm": "Glazed", "is_stockout": True, "stockout_hour": 14},
        {"item_cd": "SKU_B", "item_nm": "Boston", "is_stockout": False, "stockout_hour": None},
    ]
    repository._fetch_inventory_rows_for_date = lambda store_id, sale_date: [  # type: ignore[method-assign]
        {"item_cd": "SKU_A", "item_nm": "Glazed"},
        {"item_cd": "SKU_B", "item_nm": "Boston"},
        {"item_cd": "SKU_C", "item_nm": "Choco"},
    ]
    repository._fetch_hourly_sales_rows_for_date = lambda store_id, sale_date: [  # type: ignore[method-assign]
        {"item_cd": "SKU_A", "item_nm": "Glazed", "tmzon_div": "11", "sale_qty": 2},
        {"item_cd": "SKU_C", "item_nm": "Choco", "tmzon_div": "10", "sale_qty": 1},
    ]

    events = repository.list_inferred_stockout_events(store_id="POC_001", sale_date="20260422")

    assert len(events) == 2
    assert events[0]["item_cd"] == "SKU_C"
    assert events[0]["rule_type"] == "hourly_zero_sales_3h"
    assert events[0]["stockout_hour"] == 11
    assert events[1]["item_cd"] == "SKU_A"
    assert events[1]["rule_type"] == "raw_stockout_time"
    assert events[1]["stockout_hour"] == 14


def test_list_inferred_stockout_events_carries_operating_hours_metadata() -> None:
    repository = ProductionRepository(engine=None)

    repository._fetch_store_operating_hours = lambda store_id: {"open_hour": 10, "close_hour": 19}  # type: ignore[method-assign]
    repository._fetch_recorded_stockout_rows_for_date = lambda store_id, sale_date: []  # type: ignore[method-assign]
    repository._fetch_inventory_rows_for_date = lambda store_id, sale_date: [  # type: ignore[method-assign]
        {"item_cd": "SKU_X", "item_nm": "Vanilla"}
    ]
    repository._fetch_hourly_sales_rows_for_date = lambda store_id, sale_date: [  # type: ignore[method-assign]
        {"item_cd": "SKU_X", "item_nm": "Vanilla", "tmzon_div": "10", "sale_qty": 2},
        {"item_cd": "SKU_X", "item_nm": "Vanilla", "tmzon_div": "11", "sale_qty": 1},
    ]

    events = repository.list_inferred_stockout_events(store_id="POC_002", sale_date="20260422")

    assert len(events) == 1
    event = events[0]
    assert event["open_hour"] == 10
    assert event["close_hour"] == 19
    assert event["evidence_start_hour"] == 12
    assert event["evidence_end_hour"] == 14


def test_build_inventory_metric_row_uses_inventory_snapshot_for_stock_rate() -> None:
    repository = ProductionRepository(engine=None)

    row = repository._build_inventory_metric_row(
        {
            "masked_stor_cd": "POC_001",
            "stock_dt": "20260310",
            "item_cd": "SKU_C",
            "item_nm": "Choco",
            "stock_qty": -4,
            "sale_qty": 20,
        },
        1,
    )

    assert row["ord_avg"] == 16.0
    assert row["sal_avg"] == 20.0
    assert row["stk_avg"] == -4.0
    assert row["stk_rt"] == -0.2


def test_resolve_active_item_keys_keeps_sales_items_only_when_store_can_produce_them() -> None:
    active_keys = ProductionRepository._resolve_active_item_keys(
        recent_sales_keys={"SKU_A", "Americano", "SKU_B", "Latte"},
        recent_production_keys={"SKU_C", "Bagel"},
        direct_production_keys={"SKU_A", "Americano", "SKU_D", "Muffin"},
    )

    assert active_keys == {"SKU_A", "Americano", "SKU_C", "Bagel"}
