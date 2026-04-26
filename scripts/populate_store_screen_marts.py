from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.repositories.production_repository import ProductionRepository


DEFAULT_STORE_ID = None
DEFAULT_START_DATE = "20250311"
DEFAULT_END_DATE = "20260310"
SQL_ROOT = Path(__file__).resolve().parent / "sql"

DDL_TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "db" / "migrations" / "0029_recreate_poc_010_screen_marts.sql"


def resolve_store_ids(engine, *, requested_store_id: str | None, all_stores: bool) -> list[str]:
    if requested_store_id and not all_stores:
        return [requested_store_id.strip().upper()]
    candidate_sqls = [
        "SELECT DISTINCT CAST(masked_stor_cd AS TEXT) AS store_id FROM raw_store_master WHERE CAST(masked_stor_cd AS TEXT) LIKE 'POC_%' ORDER BY 1",
        "SELECT DISTINCT CAST(masked_stor_cd AS TEXT) AS store_id FROM raw_daily_store_item WHERE CAST(masked_stor_cd AS TEXT) LIKE 'POC_%' ORDER BY 1",
    ]
    with engine.connect() as connection:
        for sql in candidate_sqls:
            try:
                rows = connection.execute(text(sql)).scalars().all()
            except Exception:
                rows = []
            values = [str(row).strip().upper() for row in rows if str(row or '').strip()]
            if values:
                return values
    raise RuntimeError('No target store_ids found from raw source tables.')


def render_store_sql(sql: str, *, store_id: str) -> str:
    store_lower = store_id.lower()
    replacements = {
        'mart_ordering_join_poc_010': f'mart_{store_lower}_ordering_join',
        'mart_poc_010_analytics_daily': f'mart_{store_lower}_analytics_daily',
        'mart_poc_010_analytics_hourly': f'mart_{store_lower}_analytics_hourly',
        'mart_poc_010_analytics_deadline': f'mart_{store_lower}_analytics_deadline',
        'mart_poc_010_production_inventory_status': f'mart_{store_lower}_production_inventory_status',
        'mart_poc_010_production_waste_daily': f'mart_{store_lower}_production_waste_daily',
        'mart_poc_010_production_waste_monthly': f'mart_{store_lower}_production_waste_monthly',
        "DEFAULT 'POC_010'": f"DEFAULT '{store_id}'",
    }
    rendered = sql
    for old, new in replacements.items():
        rendered = rendered.replace(old, new)
    return rendered


def ensure_store_screen_mart_tables(connection, *, store_id: str) -> None:
    sql = DDL_TEMPLATE_PATH.read_text(encoding='utf-8')
    for statement in split_statements(render_store_sql(sql, store_id=store_id)):
        connection.execute(text(statement))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate store screen marts")
    parser.add_argument("--store-id", default=DEFAULT_STORE_ID)
    parser.add_argument("--all-stores", action="store_true")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    return parser.parse_args()


def normalize_yyyymmdd(value: str) -> str:
    text_value = (value or "").strip()
    if not text_value:
        raise ValueError("date is required")
    if "-" in text_value:
        return datetime.strptime(text_value, "%Y-%m-%d").strftime("%Y%m%d")
    return datetime.strptime(text_value, "%Y%m%d").strftime("%Y%m%d")


def iter_dates(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    values: list[str] = []
    cursor = start
    while cursor <= end:
        values.append(cursor.strftime("%Y%m%d"))
        cursor += timedelta(days=1)
    return values


def read_sql(name: str) -> str:
    return (SQL_ROOT / name).read_text(encoding="utf-8")


def split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    idx = 0
    while idx < len(sql):
        ch = sql[idx]
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current.append(ch)
            idx += 1
            continue
        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
            idx += 1
            continue
        if ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            idx += 1
            continue
        current.append(ch)
        idx += 1
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def load_queries(name: str, *, store_id: str | None = None) -> list:
    sql = read_sql(name)
    if store_id:
        sql = render_store_sql(sql, store_id=store_id)
    return [text(statement) for statement in split_statements(sql)]


def print_step(step: str, started_at: float, rowcount: int | None = None) -> None:
    elapsed = time.perf_counter() - started_at
    row_text = f" rows={rowcount}" if rowcount is not None and rowcount >= 0 else ""
    print(f"[ok] {step} elapsed_sec={elapsed:.2f}{row_text}")


def run_sql(connection, step: str, sql_name: str, params: dict[str, object], *, store_id: str | None = None, rowcount_expected: bool = True) -> None:
    started_at = time.perf_counter()
    result = None
    for query in load_queries(sql_name, store_id=store_id):
        result = connection.execute(query, params)
    print_step(step, started_at, result.rowcount if rowcount_expected else None)


def populate_waste_daily(connection, *, store_id: str, start_date: str, end_date: str) -> None:
    started_at = time.perf_counter()
    lookback_start = (
        datetime.strptime(start_date, "%Y%m%d").date() - timedelta(days=60)
    ).strftime("%Y%m%d")

    connection.execute(
        text(
            """
            DELETE FROM mart_poc_010_production_waste_daily
            WHERE target_date BETWEEN :start_date AND :end_date
            """
        ),
        {"start_date": start_date, "end_date": end_date},
    )

    production_rows = (
        connection.execute(
            load_queries("poc_010_production_waste_source_production.sql", store_id=store_id)[0],
            {"store_id": store_id, "lookback_start": lookback_start, "end_date": end_date},
        )
        .mappings()
        .all()
    )
    sales_rows = (
        connection.execute(
            load_queries("poc_010_production_waste_source_sales.sql", store_id=store_id)[0],
            {"store_id": store_id, "lookback_start": lookback_start, "end_date": end_date},
        )
        .mappings()
        .all()
    )
    unit_price_rows = (
        connection.execute(
            load_queries("poc_010_production_waste_source_unit_price.sql", store_id=store_id)[0],
            {"store_id": store_id, "lookback_start": lookback_start, "end_date": end_date},
        )
        .mappings()
        .all()
    )

    repository = ProductionRepository(get_database_engine())
    shelf_life_map = repository.get_shelf_life_days_map(
        item_codes=[str(row.get("item_cd") or "").strip() for row in production_rows],
        item_names=[str(row.get("item_nm") or "").strip() for row in production_rows],
    )
    unit_price_map: dict[str, float] = {}
    for row in unit_price_rows:
        item_cd = str(row.get("item_cd") or "").strip()
        item_nm = str(row.get("item_nm") or "").strip()
        normalized_name_key = repository._normalize_menu_name_key(item_nm)
        avg_unit_price = repository._safe_float(row.get("avg_unit_price"))
        for key in (item_cd, item_nm, normalized_name_key):
            if not key:
                continue
            current_price = repository._safe_float(unit_price_map.get(key))
            if key not in unit_price_map or (current_price <= 0 and avg_unit_price > 0):
                unit_price_map[key] = avg_unit_price

    upsert_sql = load_queries("poc_010_production_waste_daily_upsert.sql", store_id=store_id)[0]
    inserted = 0
    for target_date in iter_dates(start_date, end_date):
        waste_rows = ProductionRepository._compute_expiry_waste_rows(
            production_rows=[dict(r) for r in production_rows],
            sales_rows=[dict(r) for r in sales_rows],
            unit_price_map=unit_price_map,
            shelf_life_map=shelf_life_map,
            date_from=target_date,
            date_to=target_date,
        )
        for row in waste_rows:
            connection.execute(
                upsert_sql,
                {
                    "store_id": store_id,
                    "target_date": target_date,
                    "item_cd": row["item_cd"],
                    "item_nm": row["item_nm"],
                    "total_waste_qty": row["total_waste_qty"],
                    "total_waste_amount": row["total_waste_amount"],
                    "avg_cost": row["avg_cost"],
                    "adjusted_loss_qty": row["total_waste_qty"],
                    "adjusted_loss_amount": row["total_waste_amount"],
                    "estimated_expiry_loss_qty": row["total_waste_qty"],
                    "assumed_shelf_life_days": shelf_life_map.get(row["item_cd"])
                    or shelf_life_map.get(row["item_nm"])
                    or 1,
                    "expiry_risk_level": "high" if row["total_waste_qty"] > 0 else "low",
                },
            )
            inserted += 1

    print_step("production_waste_daily", started_at, inserted)


def populate_sales_margin_daily(connection, *, store_id: str, start_date: str, end_date: str) -> None:
    started_at = time.perf_counter()
    upsert_sql = load_queries("poc_010_sales_margin_daily_upsert.sql", store_id=store_id)[0]
    processed = 0
    for target_date in iter_dates(start_date, end_date):
        window_start = (
            datetime.strptime(target_date, "%Y%m%d").date() - timedelta(days=27)
        ).strftime("%Y%m%d")
        connection.execute(
            upsert_sql,
            {
                "store_id": store_id,
                "target_date": target_date,
                "window_start": window_start,
            },
        )
        processed += 1
    print_step("sales_margin_daily", started_at, processed)


def populate_waste_monthly(connection, *, store_id: str, start_date: str, end_date: str) -> None:
    start_month = f"{start_date[:4]}-{start_date[4:6]}"
    end_month = f"{end_date[:4]}-{end_date[4:6]}"
    run_sql(
        connection,
        step="production_waste_monthly",
        sql_name="poc_010_production_waste_monthly_refresh.sql",
        params={
            "store_id": store_id,
            "start_date": start_date,
            "end_date": end_date,
            "start_month": start_month,
            "end_month": end_month,
        },
    )


def main() -> None:
    args = parse_args()
    requested_store_id = (args.store_id or '').strip().upper() or None
    start_date = normalize_yyyymmdd(args.start_date or DEFAULT_START_DATE)
    end_date = normalize_yyyymmdd(args.end_date or DEFAULT_END_DATE)

    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    store_ids = resolve_store_ids(engine, requested_store_id=requested_store_id, all_stores=bool(args.all_stores or not requested_store_id))
    overall_started_at = time.perf_counter()
    for store_id in store_ids:
        store_started_at = time.perf_counter()
        with engine.begin() as connection:
            ensure_store_screen_mart_tables(connection, store_id=store_id)
            run_sql(
                connection,
                step=f"{store_id}:analytics_daily",
                sql_name="poc_010_analytics_daily_refresh.sql",
                params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
                store_id=store_id,
            )
            run_sql(
                connection,
                step=f"{store_id}:analytics_hourly",
                sql_name="poc_010_analytics_hourly_refresh.sql",
                params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
                store_id=store_id,
            )
            run_sql(
                connection,
                step=f"{store_id}:analytics_deadline",
                sql_name="poc_010_analytics_deadline_refresh.sql",
                params={"store_id": store_id},
                store_id=store_id,
            )
            run_sql(
                connection,
                step=f"{store_id}:store_weather_daily",
                sql_name="poc_010_store_weather_refresh.sql",
                params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
                store_id=store_id,
            )
            run_sql(
                connection,
                step=f"{store_id}:ordering_join",
                sql_name="poc_010_ordering_join_refresh.sql",
                params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
                store_id=store_id,
            )
            run_sql(
                connection,
                step=f"{store_id}:production_inventory_status",
                sql_name="poc_010_production_inventory_status_refresh.sql",
                params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
                store_id=store_id,
            )
            populate_waste_daily(
                connection,
                store_id=store_id,
                start_date=start_date,
                end_date=end_date,
            )
            populate_waste_monthly(
                connection,
                store_id=store_id,
                start_date=start_date,
                end_date=end_date,
            )
            populate_sales_margin_daily(
                connection,
                store_id=store_id,
                start_date=start_date,
                end_date=end_date,
            )
        store_elapsed = time.perf_counter() - store_started_at
        print(f"[ok] store marts populated store_id={store_id} start_date={start_date} end_date={end_date} elapsed_sec={store_elapsed:.2f}")

    total_elapsed = time.perf_counter() - overall_started_at
    print(
        "[ok] store screen marts populated "
        f"stores={len(store_ids)} start_date={start_date} end_date={end_date} "
        f"elapsed_sec={total_elapsed:.2f} db={get_safe_database_url()}"
    )


if __name__ == "__main__":
    main()
