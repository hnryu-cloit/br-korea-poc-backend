from __future__ import annotations

from app.repositories.production_repository import ProductionRepository


def test_compute_expiry_waste_rows_uses_expired_remaining_qty() -> None:
    rows = ProductionRepository._compute_expiry_waste_rows(
        production_rows=[
            {
                "prod_dt": "20260401",
                "item_cd": "SKU-001",
                "item_nm": "Glazed",
                "produced_qty": 10,
            },
            {
                "prod_dt": "20260402",
                "item_cd": "SKU-001",
                "item_nm": "Glazed",
                "produced_qty": 5,
            },
        ],
        sales_rows=[
            {
                "sale_dt": "20260402",
                "item_cd": "SKU-001",
                "item_nm": "Glazed",
                "sale_qty": 4,
            },
            {
                "sale_dt": "20260403",
                "item_cd": "SKU-001",
                "item_nm": "Glazed",
                "sale_qty": 2,
            },
        ],
        unit_price_map={"SKU-001": 3000},
        shelf_life_map={"SKU-001": 2},
        date_from="20260401",
        date_to="20260403",
    )

    assert rows == [
        {
            "item_cd": "SKU-001",
            "item_nm": "Glazed",
            "total_waste_qty": 9.0,
            "total_waste_amount": 27000.0,
            "avg_cost": 3000.0,
        }
    ]


def test_compute_expiry_waste_rows_skips_nonexpired_remaining_qty() -> None:
    rows = ProductionRepository._compute_expiry_waste_rows(
        production_rows=[
            {
                "prod_dt": "20260403",
                "item_cd": "SKU-002",
                "item_nm": "Choco",
                "produced_qty": 8,
            },
        ],
        sales_rows=[],
        unit_price_map={"SKU-002": 2500},
        shelf_life_map={"SKU-002": 3},
        date_from="20260403",
        date_to="20260403",
    )

    assert rows == []


def test_compute_expiry_waste_rows_matches_jbod_name_to_normalized_unit_price() -> None:
    rows = ProductionRepository._compute_expiry_waste_rows(
        production_rows=[
            {
                "prod_dt": "20260401",
                "item_cd": "SKU-JBOD",
                "item_nm": "[JBOD]카카오하니딥먼치킨",
                "produced_qty": 3,
            },
        ],
        sales_rows=[],
        unit_price_map={"카카오하니딥먼치킨": 1800, "카카오하니딥먼치킨".lower(): 1800},
        shelf_life_map={"SKU-JBOD": 1},
        date_from="20260401",
        date_to="20260401",
    )

    assert rows == [
        {
            "item_cd": "SKU-JBOD",
            "item_nm": "[JBOD]카카오하니딥먼치킨",
            "total_waste_qty": 3.0,
            "total_waste_amount": 5400.0,
            "avg_cost": 1800.0,
        }
    ]


def test_compute_expiry_waste_rows_prefers_positive_unit_price_over_zero_duplicate_key() -> None:
    rows = ProductionRepository._compute_expiry_waste_rows(
        production_rows=[
            {
                "prod_dt": "20260401",
                "item_cd": "SKU-003",
                "item_nm": "[JBOD]Glazed",
                "produced_qty": 2,
            },
        ],
        sales_rows=[],
        unit_price_map={
            "SKU-003": 0,
            "[JBOD]Glazed": 0,
            "glazed": 1900,
        },
        shelf_life_map={"SKU-003": 1},
        date_from="20260401",
        date_to="20260401",
    )

    assert rows == [
        {
            "item_cd": "SKU-003",
            "item_nm": "[JBOD]Glazed",
            "total_waste_qty": 2.0,
            "total_waste_amount": 3800.0,
            "avg_cost": 1900.0,
        }
    ]
