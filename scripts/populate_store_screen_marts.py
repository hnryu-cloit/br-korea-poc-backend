from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config.store_mart_mapping import get_store_mart_table
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.repositories.production_repository import ProductionRepository


DEFAULT_STORE_ID = "POC_010"
DEFAULT_START_DATE = "20250311"
DEFAULT_END_DATE = "20260310"
SQL_ROOT = Path(__file__).resolve().parent / "sql"

TEMPLATE_TABLES = {
    "analytics_daily": "mart_poc_010_analytics_daily",
    "analytics_hourly": "mart_poc_010_analytics_hourly",
    "analytics_deadline": "mart_poc_010_analytics_deadline",
    "ordering_join": "mart_ordering_join_poc_010",
    "production_inventory_status": "mart_poc_010_production_inventory_status",
    "production_waste_daily": "mart_poc_010_production_waste_daily",
    "production_waste_monthly": "mart_poc_010_production_waste_monthly",
}

SQL_TABLE_TEMPLATES = {
    "poc_010_analytics_daily_refresh.sql": TEMPLATE_TABLES["analytics_daily"],
    "poc_010_analytics_hourly_refresh.sql": TEMPLATE_TABLES["analytics_hourly"],
    "poc_010_analytics_deadline_refresh.sql": TEMPLATE_TABLES["analytics_deadline"],
    "poc_010_ordering_join_refresh.sql": TEMPLATE_TABLES["ordering_join"],
    "poc_010_production_inventory_status_refresh.sql": TEMPLATE_TABLES["production_inventory_status"],
    "poc_010_production_waste_daily_upsert.sql": TEMPLATE_TABLES["production_waste_daily"],
    "poc_010_production_waste_monthly_refresh.sql": TEMPLATE_TABLES["production_waste_monthly"],
}

SQL_TARGET_TABLE_KEYS = {
    "poc_010_analytics_daily_refresh.sql": "analytics_daily",
    "poc_010_analytics_hourly_refresh.sql": "analytics_hourly",
    "poc_010_analytics_deadline_refresh.sql": "analytics_deadline",
    "poc_010_ordering_join_refresh.sql": "ordering_join",
    "poc_010_production_inventory_status_refresh.sql": "production_inventory_status",
    "poc_010_production_waste_daily_upsert.sql": "production_waste_daily",
    "poc_010_production_waste_monthly_refresh.sql": "production_waste_monthly",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate screen marts for one or more stores")
    parser.add_argument("--store-id", action="append", dest="store_ids")
    parser.add_argument("--exclude-store-id", action="append", dest="excluded_store_ids")
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


def target_tables(store_id: str) -> dict[str, str]:
    normalized_store_id = store_id.strip().upper()
    store_key = normalized_store_id.lower()
    return {
        "analytics_daily": get_store_mart_table(normalized_store_id, "analytics", "daily_table")
        or f"mart_{store_key}_analytics_daily",
        "analytics_hourly": get_store_mart_table(normalized_store_id, "analytics", "hourly_table")
        or f"mart_{store_key}_analytics_hourly",
        "analytics_deadline": get_store_mart_table(normalized_store_id, "analytics", "deadline_table")
        or f"mart_{store_key}_analytics_deadline",
        "ordering_join": get_store_mart_table(normalized_store_id, "ordering", "options_join_table")
        or f"mart_{store_key}_ordering_join",
        "production_inventory_status": get_store_mart_table(
            normalized_store_id, "production", "inventory_status_table"
        )
        or f"mart_{store_key}_production_inventory_status",
        "production_waste_daily": get_store_mart_table(
            normalized_store_id, "production", "waste_daily_table"
        )
        or f"mart_{store_key}_production_waste_daily",
        "production_waste_monthly": get_store_mart_table(
            normalized_store_id, "production", "waste_monthly_table"
        )
        or f"mart_{store_key}_production_waste_monthly",
    }


def load_queries(name: str, *, store_id: str) -> list:
    sql = read_sql(name)
    replacements = target_tables(store_id)
    if name in SQL_TABLE_TEMPLATES:
        sql = sql.replace(SQL_TABLE_TEMPLATES[name], replacements[SQL_TARGET_TABLE_KEYS[name]])
    return [text(statement) for statement in split_statements(sql)]


def ensure_store_screen_tables(connection, *, store_id: str) -> None:
    for key, target_table in target_tables(store_id).items():
        template_table = TEMPLATE_TABLES[key]
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {target_table}
                (LIKE {template_table} INCLUDING ALL)
                """
            )
        )


def print_step(step: str, started_at: float, rowcount: int | None = None) -> None:
    elapsed = time.perf_counter() - started_at
    row_text = f" rows={rowcount}" if rowcount is not None and rowcount >= 0 else ""
    print(f"[ok] {step} elapsed_sec={elapsed:.2f}{row_text}")


def run_sql(
    connection,
    *,
    store_id: str,
    step: str,
    sql_name: str,
    params: dict[str, object],
    rowcount_expected: bool = True,
) -> None:
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
    tables = target_tables(store_id)

    connection.execute(
        text(
            f"""
            DELETE FROM {tables['production_waste_daily']}
            WHERE target_date BETWEEN :start_date AND :end_date
            """
        ),
        {"start_date": start_date, "end_date": end_date},
    )

    production_rows = (
        connection.execute(
            text(read_sql("poc_010_production_waste_source_production.sql")),
            {"store_id": store_id, "lookback_start": lookback_start, "end_date": end_date},
        )
        .mappings()
        .all()
    )
    sales_rows = (
        connection.execute(
            text(read_sql("poc_010_production_waste_source_sales.sql")),
            {"store_id": store_id, "lookback_start": lookback_start, "end_date": end_date},
        )
        .mappings()
        .all()
    )
    unit_price_rows = (
        connection.execute(
            text(read_sql("poc_010_production_waste_source_unit_price.sql")),
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
        if item_cd and item_cd not in unit_price_map:
            unit_price_map[item_cd] = avg_unit_price
        if item_nm and item_nm not in unit_price_map:
            unit_price_map[item_nm] = avg_unit_price
        if normalized_name_key and normalized_name_key not in unit_price_map:
            unit_price_map[normalized_name_key] = avg_unit_price

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
    upsert_sql = text(read_sql("poc_010_sales_margin_daily_upsert.sql"))
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
        store_id=store_id,
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


def fetch_all_store_ids(connection) -> list[str]:
    rows = (
        connection.execute(
            text(
                """
                SELECT DISTINCT masked_stor_cd AS store_id
                FROM raw_store_master
                WHERE NULLIF(TRIM(masked_stor_cd), '') IS NOT NULL
                ORDER BY masked_stor_cd
                """
            )
        )
        .mappings()
        .all()
    )
    return [str(row["store_id"]).strip().upper() for row in rows if str(row["store_id"]).strip()]


def resolve_store_ids(connection, args: argparse.Namespace) -> list[str]:
    requested = [str(value).strip().upper() for value in (args.store_ids or []) if str(value).strip()]
    excluded = {str(value).strip().upper() for value in (args.excluded_store_ids or []) if str(value).strip()}

    if args.all_stores:
        resolved = fetch_all_store_ids(connection)
    elif requested:
        resolved = requested
    else:
        resolved = [DEFAULT_STORE_ID]
    return [store_id for store_id in resolved if store_id not in excluded]


def populate_store(connection, *, store_id: str, start_date: str, end_date: str) -> None:
    ensure_store_screen_tables(connection, store_id=store_id)
    run_sql(
        connection,
        store_id=store_id,
        step="analytics_daily",
        sql_name="poc_010_analytics_daily_refresh.sql",
        params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
    )
    run_sql(
        connection,
        store_id=store_id,
        step="analytics_hourly",
        sql_name="poc_010_analytics_hourly_refresh.sql",
        params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
    )
    run_sql(
        connection,
        store_id=store_id,
        step="analytics_deadline",
        sql_name="poc_010_analytics_deadline_refresh.sql",
        params={"store_id": store_id},
    )
    run_sql(
        connection,
        store_id=store_id,
        step="store_weather_daily",
        sql_name="poc_010_store_weather_refresh.sql",
        params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
    )
    run_sql(
        connection,
        store_id=store_id,
        step="ordering_join",
        sql_name="poc_010_ordering_join_refresh.sql",
        params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
    )
    run_sql(
        connection,
        store_id=store_id,
        step="production_inventory_status",
        sql_name="poc_010_production_inventory_status_refresh.sql",
        params={"store_id": store_id, "start_date": start_date, "end_date": end_date},
    )
    populate_waste_daily(connection, store_id=store_id, start_date=start_date, end_date=end_date)
    populate_waste_monthly(connection, store_id=store_id, start_date=start_date, end_date=end_date)
    populate_sales_margin_daily(connection, store_id=store_id, start_date=start_date, end_date=end_date)


def main() -> None:
    args = parse_args()
    start_date = normalize_yyyymmdd(args.start_date or DEFAULT_START_DATE)
    end_date = normalize_yyyymmdd(args.end_date or DEFAULT_END_DATE)

    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    with engine.connect() as connection:
        store_ids = resolve_store_ids(connection, args)

    overall_started_at = time.perf_counter()
    failures: list[tuple[str, str]] = []
    processed = 0
    for store_id in store_ids:
        started_at = time.perf_counter()
        try:
            with engine.begin() as connection:
                populate_store(connection, store_id=store_id, start_date=start_date, end_date=end_date)
            elapsed = time.perf_counter() - started_at
            processed += 1
            print(f"[ok] store completed store_id={store_id} elapsed_sec={elapsed:.2f}")
        except Exception as exc:  # noqa: BLE001
            failures.append((store_id, str(exc)))
            print(f"[error] store failed store_id={store_id} error={exc}")

    total_elapsed = time.perf_counter() - overall_started_at
    print(
        "[ok] store screen marts population finished "
        f"processed={processed} failed={len(failures)} elapsed_sec={total_elapsed:.2f} "
        f"db={get_safe_database_url()}"
    )
    if failures:
        for store_id, message in failures:
            print(f"[error] failed_store store_id={store_id} message={message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
