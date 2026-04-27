from __future__ import annotations

from app.repositories.production_repository import ProductionRepository


def test_build_ranked_row_keeps_first_and_second_production_distinct() -> None:
    repository = ProductionRepository(engine=None)

    row = repository._build_ranked_row(
        "SKU-003",
        production={"qty": 12, "item_nm": "Test Choco", "item_cd": "SKU-003"},
        secondary={"qty": 5, "item_nm": "Test Choco", "item_cd": "SKU-003"},
        stock={"qty": 3, "item_nm": "Test Choco", "item_cd": "SKU-003"},
        sale={"qty": 8, "item_nm": "Test Choco", "item_cd": "SKU-003"},
        order_confirm={},
        hourly_sale={},
    )

    assert row["prod1"] != row["prod2"]


def test_resolve_active_item_keys_limits_production_status_scope() -> None:
    active_keys = ProductionRepository._resolve_active_item_keys(
        recent_production_keys={"PROD_ONLY", "BOTH"},
        direct_production_keys={"BOTH", "OTHER_DIRECT"},
    )

    assert active_keys == {"BOTH"}

def test_resolve_recent_window_bounds_excludes_today() -> None:
    date_from, date_to = ProductionRepository._resolve_recent_window_bounds(
        reference_date="2026-03-05",
        window_days=7,
    )

    assert date_from == "20260226"
    assert date_to == "20260304"
