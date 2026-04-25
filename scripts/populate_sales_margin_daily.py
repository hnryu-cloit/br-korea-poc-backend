from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate daily sales margin mart")
    parser.add_argument("--store-id", required=True)
    parser.add_argument("--start-date", help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--end-date", help="YYYY-MM-DD or YYYYMMDD")
    return parser.parse_args()


def normalize_yyyymmdd(value: str) -> str:
    text_value = (value or "").strip()
    if not text_value:
        raise ValueError("date is required")
    if "-" in text_value:
        return datetime.strptime(text_value, "%Y-%m-%d").strftime("%Y%m%d")
    return datetime.strptime(text_value, "%Y%m%d").strftime("%Y%m%d")


def daterange(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    days: list[str] = []
    cursor = start
    while cursor <= end:
        days.append(cursor.strftime("%Y%m%d"))
        cursor += timedelta(days=1)
    return days


def resolve_date_bounds(connection, store_id: str, start_date: str | None, end_date: str | None) -> tuple[str, str]:
    if start_date and end_date:
        return normalize_yyyymmdd(start_date), normalize_yyyymmdd(end_date)

    bounds = (
        connection.execute(
            text(
                """
                SELECT MIN(sale_dt) AS min_dt, MAX(sale_dt) AS max_dt
                FROM raw_daily_store_item
                WHERE masked_stor_cd = :store_id
                """
            ),
            {"store_id": store_id},
        )
        .mappings()
        .first()
    )
    if not bounds or not bounds["min_dt"] or not bounds["max_dt"]:
        raise RuntimeError(f"No sales rows found for store_id={store_id}")

    resolved_start = normalize_yyyymmdd(start_date) if start_date else str(bounds["min_dt"])
    resolved_end = normalize_yyyymmdd(end_date) if end_date else str(bounds["max_dt"])
    return resolved_start, resolved_end


def main() -> None:
    args = parse_args()
    store_id = (args.store_id or "").strip()
    if not store_id:
        raise RuntimeError("store_id is required")

    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    upsert_sql = text(
        """
        INSERT INTO mart_sales_margin_daily (
            store_id,
            target_date,
            window_start_date,
            window_end_date,
            avg_margin_rate,
            avg_net_profit_per_item,
            product_count,
            generated_at,
            updated_at
        )
        WITH sold_products AS (
            SELECT DISTINCT COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '') AS item_nm
            FROM raw_daily_store_item
            WHERE masked_stor_cd = :store_id
              AND sale_dt >= :window_start
              AND sale_dt <= :target_date
              AND COALESCE(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC), 0) > 0
        ),
        product_margin AS (
            SELECT
                COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '') AS item_nm,
                AVG(
                    (CAST(p.sale_prc AS NUMERIC) - CAST(p.item_cost AS NUMERIC))
                    / NULLIF(CAST(p.sale_prc AS NUMERIC), 0)
                ) AS margin_rate,
                AVG(CAST(p.sale_prc AS NUMERIC) - CAST(p.item_cost AS NUMERIC)) AS net_profit_per_item
            FROM raw_production_extract p
            JOIN sold_products s
              ON s.item_nm = COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '')
            WHERE p.masked_stor_cd = :store_id
              AND p.prod_dt >= :window_start
              AND p.prod_dt <= :target_date
              AND CAST(p.sale_prc AS NUMERIC) > 0
              AND CAST(p.item_cost AS NUMERIC) > 0
            GROUP BY COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '')
        )
        SELECT
            :store_id,
            :target_date,
            :window_start,
            :target_date,
            COALESCE(AVG(margin_rate), 0),
            COALESCE(AVG(net_profit_per_item), 0),
            COUNT(*),
            NOW(),
            NOW()
        FROM product_margin
        ON CONFLICT (store_id, target_date)
        DO UPDATE SET
            window_start_date = EXCLUDED.window_start_date,
            window_end_date = EXCLUDED.window_end_date,
            avg_margin_rate = EXCLUDED.avg_margin_rate,
            avg_net_profit_per_item = EXCLUDED.avg_net_profit_per_item,
            product_count = EXCLUDED.product_count,
            generated_at = EXCLUDED.generated_at,
            updated_at = NOW()
        """
    )

    with engine.begin() as connection:
        start_date, end_date = resolve_date_bounds(connection, store_id, args.start_date, args.end_date)
        target_dates = daterange(start_date, end_date)
        processed = 0
        for target_date in target_dates:
            target_day = datetime.strptime(target_date, "%Y%m%d").date()
            window_start = (target_day - timedelta(days=27)).strftime("%Y%m%d")
            connection.execute(
                upsert_sql,
                {
                    "store_id": store_id,
                    "target_date": target_date,
                    "window_start": window_start,
                },
            )
            processed += 1

    print(
        f"[ok] mart_sales_margin_daily populated "
        f"store_id={store_id} start_date={start_date} end_date={end_date} days={processed}"
    )


if __name__ == "__main__":
    main()
