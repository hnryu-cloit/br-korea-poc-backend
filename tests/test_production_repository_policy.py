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


def test_infer_stockout_from_hourly_sales_marks_first_hour_of_three_zero_hours() -> None:
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
    assert stockout_map["SKU_A"]["stockout_hour"] == 10


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
