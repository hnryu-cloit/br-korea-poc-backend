from __future__ import annotations

from sqlalchemy import create_engine, text

from app.repositories.production_repository import ProductionRepository


def test_fifo_lot_summary_uses_month_to_previous_day_window() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE inventory_fifo_lots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    masked_stor_cd TEXT NOT NULL,
                    item_cd TEXT,
                    item_nm TEXT NOT NULL,
                    lot_type TEXT NOT NULL,
                    lot_date DATE NOT NULL,
                    expiry_date DATE,
                    shelf_life_days INTEGER,
                    initial_qty NUMERIC NOT NULL DEFAULT 0,
                    consumed_qty NUMERIC NOT NULL DEFAULT 0,
                    wasted_qty NUMERIC NOT NULL DEFAULT 0,
                    unit_cost NUMERIC NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active'
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO inventory_fifo_lots (
                    masked_stor_cd,
                    item_cd,
                    item_nm,
                    lot_type,
                    lot_date,
                    expiry_date,
                    shelf_life_days,
                    initial_qty,
                    consumed_qty,
                    wasted_qty,
                    status
                ) VALUES
                    ('POC_010', 'SKU-001', 'Glazed', 'production', '2026-03-01', '2026-03-02', 1, 10, 4, 0, 'active'),
                    ('POC_010', 'SKU-001', 'Glazed', 'production', '2026-03-04', '2026-03-05', 1, 6, 2, 1, 'expired'),
                    ('POC_010', 'SKU-001', 'Glazed', 'production', '2026-03-05', '2026-03-06', 1, 8, 1, 0, 'active'),
                    ('POC_010', 'SKU-002', 'Coffee', 'delivery', '2026-03-02', '2026-03-02', 0, 5, 3, 0, 'active')
                """
            )
        )

    repository = ProductionRepository(engine=engine)
    repository._get_production_inventory_mart_table = lambda _store_id: None  # type: ignore[method-assign]
    repository._production_inventory_mart_configured = lambda _store_id: False  # type: ignore[method-assign]
    repository._fetch_direct_production_item_keys = lambda **_kwargs: {"SKU-001"}  # type: ignore[method-assign]

    rows, total = repository.get_fifo_lot_summary(store_id="POC_010", page=1, page_size=20, date="2026-03-05")

    assert total == 2
    assert rows == [
        {
            "item_nm": "Glazed",
            "lot_type": "production",
            "shelf_life_days": 1,
            "last_lot_date": "2026-03-04",
            "total_initial_qty": 16,
            "total_consumed_qty": 6,
            "total_wasted_qty": 1,
            "active_remaining_qty": 6,
            "active_lot_count": 1,
            "sold_out_lot_count": 0,
            "expired_lot_count": 1,
        },
        {
            "item_nm": "Coffee",
            "lot_type": "delivery",
            "shelf_life_days": 0,
            "last_lot_date": "2026-03-02",
            "total_initial_qty": 5,
            "total_consumed_qty": 3,
            "total_wasted_qty": 0,
            "active_remaining_qty": 2,
            "active_lot_count": 1,
            "sold_out_lot_count": 0,
            "expired_lot_count": 0,
        },
    ]


def test_fifo_lot_summary_returns_empty_on_first_day_of_month() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE inventory_fifo_lots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    masked_stor_cd TEXT NOT NULL,
                    item_cd TEXT,
                    item_nm TEXT NOT NULL,
                    lot_type TEXT NOT NULL,
                    lot_date DATE NOT NULL,
                    expiry_date DATE,
                    shelf_life_days INTEGER,
                    initial_qty NUMERIC NOT NULL DEFAULT 0,
                    consumed_qty NUMERIC NOT NULL DEFAULT 0,
                    wasted_qty NUMERIC NOT NULL DEFAULT 0,
                    unit_cost NUMERIC NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active'
                )
                """
            )
        )

    repository = ProductionRepository(engine=engine)
    repository._get_production_inventory_mart_table = lambda _store_id: None  # type: ignore[method-assign]
    repository._production_inventory_mart_configured = lambda _store_id: False  # type: ignore[method-assign]

    rows, total = repository.get_fifo_lot_summary(store_id="POC_010", page=1, page_size=20, date="2026-03-01")

    assert rows == []
    assert total == 0


def test_fifo_lot_summary_falls_back_to_inventory_table_when_mart_is_empty() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE inventory_fifo_lots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    masked_stor_cd TEXT NOT NULL,
                    item_cd TEXT,
                    item_nm TEXT NOT NULL,
                    lot_type TEXT NOT NULL,
                    lot_date DATE NOT NULL,
                    expiry_date DATE,
                    shelf_life_days INTEGER,
                    initial_qty NUMERIC NOT NULL DEFAULT 0,
                    consumed_qty NUMERIC NOT NULL DEFAULT 0,
                    wasted_qty NUMERIC NOT NULL DEFAULT 0,
                    unit_cost NUMERIC NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active'
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO inventory_fifo_lots (
                    masked_stor_cd, item_cd, item_nm, lot_type, lot_date, expiry_date,
                    shelf_life_days, initial_qty, consumed_qty, wasted_qty, status
                ) VALUES
                    ('POC_010', 'SKU-001', 'Glazed', 'production', '2026-03-10', '2026-03-11', 1, 10, 3, 2, 'expired')
                """
            )
        )

    repository = ProductionRepository(engine=engine)
    repository._get_fifo_lot_summary_from_inventory_mart = lambda **_kwargs: ([], 0)  # type: ignore[method-assign]
    repository._fetch_direct_production_item_keys = lambda **_kwargs: {"SKU-001"}  # type: ignore[method-assign]

    rows, total = repository.get_fifo_lot_summary(
        store_id="POC_010",
        page=1,
        page_size=20,
        date="2026-03-31",
    )

    assert total == 1
    assert rows[0]["item_nm"] == "Glazed"
    assert rows[0]["lot_type"] == "production"
    assert rows[0]["total_wasted_qty"] == 2.0


def test_fifo_lot_summary_normalizes_item_type_using_store_production_items() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE inventory_fifo_lots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    masked_stor_cd TEXT NOT NULL,
                    item_cd TEXT,
                    item_nm TEXT NOT NULL,
                    lot_type TEXT NOT NULL,
                    lot_date DATE NOT NULL,
                    expiry_date DATE,
                    shelf_life_days INTEGER,
                    initial_qty NUMERIC NOT NULL DEFAULT 0,
                    consumed_qty NUMERIC NOT NULL DEFAULT 0,
                    wasted_qty NUMERIC NOT NULL DEFAULT 0,
                    unit_cost NUMERIC NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active'
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE raw_store_production_item (
                    masked_stor_cd TEXT,
                    masked_stor_nm TEXT,
                    item_cd TEXT,
                    item_nm TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet VARCHAR(255),
                    loaded_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO raw_store_production_item (
                    masked_stor_cd, masked_stor_nm, item_cd, item_nm, source_file, source_sheet, loaded_at
                ) VALUES
                    ('POC_010', 'Store', 'SKU-100', 'Bagel', 'seed.xlsx', 'Sheet1', '2026-03-01T00:00:00+09:00')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO inventory_fifo_lots (
                    masked_stor_cd,
                    item_cd,
                    item_nm,
                    lot_type,
                    lot_date,
                    expiry_date,
                    shelf_life_days,
                    initial_qty,
                    consumed_qty,
                    wasted_qty,
                    status
                ) VALUES
                    ('POC_010', 'SKU-100', 'Bagel', 'delivery', '2026-03-02', '2026-03-03', 1, 10, 4, 0, 'active'),
                    ('POC_010', 'SKU-100', 'Bagel', 'production', '2026-03-03', '2026-03-04', 1, 5, 1, 0, 'sold_out'),
                    ('POC_010', 'SKU-200', 'Cream', 'production', '2026-03-02', '2026-03-03', 30, 7, 2, 0, 'active')
                """
            )
        )

    repository = ProductionRepository(engine=engine)
    repository._get_production_inventory_mart_table = lambda _store_id: None  # type: ignore[method-assign]
    repository._production_inventory_mart_configured = lambda _store_id: False  # type: ignore[method-assign]

    production_rows, production_total = repository.get_fifo_lot_summary(
        store_id="POC_010",
        lot_type="production",
        page=1,
        page_size=20,
        date="2026-03-05",
    )
    delivery_rows, delivery_total = repository.get_fifo_lot_summary(
        store_id="POC_010",
        lot_type="delivery",
        page=1,
        page_size=20,
        date="2026-03-05",
    )

    assert production_total == 1
    assert production_rows == [
        {
            "item_nm": "Bagel",
            "lot_type": "production",
            "shelf_life_days": 1,
            "last_lot_date": "2026-03-03",
            "total_initial_qty": 15.0,
            "total_consumed_qty": 5.0,
            "total_wasted_qty": 0.0,
            "active_remaining_qty": 6.0,
            "active_lot_count": 1,
            "sold_out_lot_count": 1,
            "expired_lot_count": 0,
        }
    ]
    assert delivery_total == 1
    assert delivery_rows == [
        {
            "item_nm": "Cream",
            "lot_type": "delivery",
            "shelf_life_days": 30,
            "last_lot_date": "2026-03-02",
            "total_initial_qty": 7.0,
            "total_consumed_qty": 2.0,
            "total_wasted_qty": 0.0,
            "active_remaining_qty": 5.0,
            "active_lot_count": 1,
            "sold_out_lot_count": 0,
            "expired_lot_count": 0,
        }
    ]
