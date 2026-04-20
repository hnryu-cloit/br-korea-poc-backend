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
