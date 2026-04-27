from __future__ import annotations

from collections import defaultdict
import logging
import re
import unicodedata
from datetime import date as date_type
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config.store_mart_mapping import get_store_mart_table, has_store_mart_mapping
from app.infrastructure.db.utils import has_table
from app.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))


def _validate_iso_date(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return date_type.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"date 파라미터는 YYYY-MM-DD 형식이어야 합니다: {value}") from exc


class ProductionRepository(BaseRepository):
    _BUSINESS_HOURS: tuple[int, ...] = tuple(range(8, 22))
    _STOCKOUT_ZERO_SALES_WINDOW = 3

    @staticmethod
    def _build_history_filters(
        store_id: str | None = None, date_from: str | None = None, date_to: str | None = None
    ) -> tuple[str, dict]:
        clauses: list[str] = []
        params: dict[str, str | int | None] = {}
        if store_id:
            clauses.append("store_id = :store_id")
            params["store_id"] = store_id
        if date_from:
            clauses.append("registered_at::date >= CAST(:date_from AS DATE)")
            params["date_from"] = date_from
        if date_to:
            clauses.append("registered_at::date <= CAST(:date_to AS DATE)")
            params["date_to"] = date_to
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where_clause, params

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    @staticmethod
    def _resolve_fifo_month_window(
        date: str | None,
    ) -> tuple[str, date_type, date_type, date_type | None]:
        target_date = _validate_iso_date(date) or datetime.now(_KST).date().isoformat()
        target_day = date_type.fromisoformat(target_date)
        month_start = target_day.replace(day=1)
        previous_day = target_day - timedelta(days=1)
        if previous_day < month_start:
            return target_date, target_day, month_start, None
        return target_date, target_day, month_start, previous_day

    def _get_production_inventory_mart_table(self, store_id: str | None) -> str | None:
        if not self.engine:
            return None
        table_name = get_store_mart_table(store_id, "production", "inventory_status_table")
        if not table_name:
            return None
        return table_name if has_table(self.engine, table_name) else None

    def _production_inventory_mart_configured(self, store_id: str | None) -> bool:
        return has_store_mart_mapping(store_id, "production", "inventory_status_table")

    def _production_waste_daily_mart_table(self, store_id: str | None) -> str | None:
        if not self.engine:
            return None
        table_name = get_store_mart_table(store_id, "production", "waste_daily_table")
        if not table_name:
            return None
        return table_name if has_table(self.engine, table_name) else None

    def _production_waste_daily_mart_configured(self, store_id: str | None) -> bool:
        return has_store_mart_mapping(store_id, "production", "waste_daily_table")

    def _fetch_fifo_production_item_keys(
        self,
        *,
        store_id: str,
        month_start_date: str,
        previous_day_date: str,
    ) -> set[str]:
        direct_keys = self._fetch_direct_production_item_keys(store_id=store_id)
        normalized_direct_keys = {
            self._normalize_item_key(key, key) for key in direct_keys if str(key or "").strip()
        }
        if normalized_direct_keys:
            return normalized_direct_keys

        if not self.engine or not has_table(self.engine, "raw_production_extract"):
            return set()

        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            """
                            SELECT DISTINCT item_cd, item_nm
                            FROM raw_production_extract
                            WHERE masked_stor_cd = :store_id
                              AND prod_dt BETWEEN :month_start_date AND :previous_day_date
                            """
                        ),
                        {
                            "store_id": store_id,
                            "month_start_date": month_start_date,
                            "previous_day_date": previous_day_date,
                        },
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return set()

        return {
            self._normalize_item_key(row.get("item_cd"), row.get("item_nm"))
            for row in rows
            if self._normalize_item_key(row.get("item_cd"), row.get("item_nm"))
        }

    def _normalize_fifo_lot_rows(
        self,
        *,
        store_id: str,
        rows: list[dict[str, object]],
        lot_type: str | None,
        page: int,
        page_size: int,
        month_start_date: str,
        previous_day_date: str,
    ) -> tuple[list[dict[str, object]], int]:
        production_keys = self._fetch_fifo_production_item_keys(
            store_id=store_id,
            month_start_date=month_start_date,
            previous_day_date=previous_day_date,
        )
        aggregated: dict[tuple[str, str], dict[str, object]] = {}

        for row in rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            item_nm = str(row.get("item_nm") or item_cd).strip()
            item_key = self._normalize_item_key(item_cd, item_nm)
            normalized_lot_type = "production" if item_key in production_keys else "delivery"
            if lot_type and normalized_lot_type != lot_type:
                continue

            aggregate_key = (item_nm, normalized_lot_type)
            current = aggregated.get(aggregate_key)
            last_lot_date = str(row.get("last_lot_date") or "")
            if current is None:
                aggregated[aggregate_key] = {
                    "item_nm": item_nm,
                    "lot_type": normalized_lot_type,
                    "shelf_life_days": row.get("shelf_life_days"),
                    "last_lot_date": last_lot_date or None,
                    "total_initial_qty": self._safe_float(row.get("total_initial_qty")),
                    "total_consumed_qty": self._safe_float(row.get("total_consumed_qty")),
                    "total_wasted_qty": self._safe_float(row.get("total_wasted_qty")),
                    "active_remaining_qty": self._safe_float(row.get("active_remaining_qty")),
                    "active_lot_count": self._safe_int(row.get("active_lot_count")),
                    "sold_out_lot_count": self._safe_int(row.get("sold_out_lot_count")),
                    "expired_lot_count": self._safe_int(row.get("expired_lot_count")),
                }
                continue

            if row.get("shelf_life_days") is not None:
                current["shelf_life_days"] = max(
                    self._safe_int(current.get("shelf_life_days")),
                    self._safe_int(row.get("shelf_life_days")),
                )
            if last_lot_date and (
                current.get("last_lot_date") is None or str(current.get("last_lot_date")) < last_lot_date
            ):
                current["last_lot_date"] = last_lot_date
            current["total_initial_qty"] = self._safe_float(current.get("total_initial_qty")) + self._safe_float(
                row.get("total_initial_qty")
            )
            current["total_consumed_qty"] = self._safe_float(current.get("total_consumed_qty")) + self._safe_float(
                row.get("total_consumed_qty")
            )
            current["total_wasted_qty"] = self._safe_float(current.get("total_wasted_qty")) + self._safe_float(
                row.get("total_wasted_qty")
            )
            current["active_remaining_qty"] = self._safe_float(
                current.get("active_remaining_qty")
            ) + self._safe_float(row.get("active_remaining_qty"))
            current["active_lot_count"] = self._safe_int(current.get("active_lot_count")) + self._safe_int(
                row.get("active_lot_count")
            )
            current["sold_out_lot_count"] = self._safe_int(
                current.get("sold_out_lot_count")
            ) + self._safe_int(row.get("sold_out_lot_count"))
            current["expired_lot_count"] = self._safe_int(current.get("expired_lot_count")) + self._safe_int(
                row.get("expired_lot_count")
            )

        normalized_rows = list(aggregated.values())
        normalized_rows.sort(
            key=lambda row: (
                -self._safe_float(row.get("total_wasted_qty")),
                str(row.get("item_nm") or ""),
            )
        )

        total = len(normalized_rows)
        offset = max(0, (page - 1) * page_size)
        paged_rows = normalized_rows[offset : offset + page_size]
        return paged_rows, total

    def _get_fifo_lot_summary_from_inventory_mart(
        self,
        *,
        store_id: str,
        lot_type: str | None,
        page: int,
        page_size: int,
        date: str | None,
    ) -> tuple[list[dict], int] | None:
        if not self.engine:
            return None

        inventory_mart_table = self._get_production_inventory_mart_table(store_id)
        if not inventory_mart_table:
            if self._production_inventory_mart_configured(store_id):
                return [], 0
            return None

        _, _, month_start, previous_day = self._resolve_fifo_month_window(date)
        if previous_day is None:
            return [], 0

        month_start_key = month_start.strftime("%Y%m%d")
        previous_day_key = previous_day.strftime("%Y%m%d")
        offset = max(0, (page - 1) * page_size)

        waste_mart_table = self._production_waste_daily_mart_table(store_id)

        try:
            with self.engine.connect() as conn:
                inventory_rows = (
                    conn.execute(
                        text(
                            f"""
                            SELECT
                                item_cd,
                                item_nm,
                                total_stock,
                                total_sold,
                                total_orderable,
                                assumed_shelf_life_days
                            FROM {inventory_mart_table}
                            WHERE store_id = :store_id
                              AND business_date = :business_date
                              AND COALESCE(total_stock, 0) > 0
                            ORDER BY total_stock DESC, item_nm ASC
                            """
                        ),
                        {"store_id": store_id, "business_date": previous_day_key},
                    )
                    .mappings()
                    .all()
                )

                monthly_sales_rows = [
                    dict(row)
                    for row in (
                        conn.execute(
                            text(
                                f"""
                                SELECT
                                    item_cd,
                                    item_nm,
                                    SUM(COALESCE(total_sold, 0)) AS total_consumed_qty
                                FROM {inventory_mart_table}
                                WHERE store_id = :store_id
                                  AND business_date BETWEEN :month_start_date AND :previous_day_date
                                GROUP BY item_cd, item_nm
                                """
                            ),
                            {
                                "store_id": store_id,
                                "month_start_date": month_start_key,
                                "previous_day_date": previous_day_key,
                            },
                        )
                        .mappings()
                        .all()
                    )
                ]

                waste_rows: list[dict[str, object]] = []
                if waste_mart_table:
                    waste_rows = [
                        dict(row)
                        for row in (
                            conn.execute(
                                text(
                                    f"""
                                    SELECT
                                        item_cd,
                                        item_nm,
                                        SUM(total_waste_qty) AS total_waste_qty
                                    FROM {waste_mart_table}
                                    WHERE store_id = :store_id
                                      AND target_date BETWEEN :month_start_date AND :previous_day_date
                                    GROUP BY item_cd, item_nm
                                    """
                                ),
                                {
                                    "store_id": store_id,
                                    "month_start_date": month_start_key,
                                    "previous_day_date": previous_day_key,
                                },
                            )
                            .mappings()
                            .all()
                        )
                    ]

                production_keys = self._fetch_fifo_production_item_keys(
                    store_id=store_id,
                    month_start_date=month_start_key,
                    previous_day_date=previous_day_key,
                )
        except SQLAlchemyError as exc:
            logger.warning(
                "inventory mart 기반 FIFO 요약 조회 실패: store_id=%s error=%s",
                store_id,
                exc,
            )
            return [], 0

        waste_qty_by_key: dict[str, float] = {}
        for row in waste_rows:
            item_key = self._normalize_item_key(row.get("item_cd"), row.get("item_nm"))
            if not item_key:
                continue
            waste_qty_by_key[item_key] = self._safe_float(row.get("total_waste_qty"))

        consumed_qty_by_key: dict[str, float] = {}
        for row in monthly_sales_rows:
            item_key = self._normalize_item_key(row.get("item_cd"), row.get("item_nm"))
            if not item_key:
                continue
            consumed_qty_by_key[item_key] = self._safe_float(row.get("total_consumed_qty"))

        materialized_rows: list[dict[str, object]] = []
        for row in inventory_rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            item_nm = str(row.get("item_nm") or item_cd).strip()
            item_key = self._normalize_item_key(item_cd, item_nm)
            month_wasted_qty = waste_qty_by_key.get(item_key, 0.0)
            month_consumed_qty = consumed_qty_by_key.get(item_key, 0.0)
            previous_day_stock = max(self._safe_float(row.get("total_stock")), 0.0)
            active_remaining_qty = previous_day_stock
            if active_remaining_qty <= 0:
                continue

            inferred_lot_type = "production" if item_key in production_keys else "delivery"
            if lot_type and inferred_lot_type != lot_type:
                continue

            total_initial_qty = max(previous_day_stock + month_consumed_qty + month_wasted_qty, 0.0)
            materialized_rows.append(
                {
                    "item_nm": item_nm,
                    "lot_type": inferred_lot_type,
                    "shelf_life_days": max(self._safe_int(row.get("assumed_shelf_life_days")), 1),
                    "last_lot_date": previous_day.isoformat(),
                    "total_initial_qty": round(total_initial_qty, 2),
                    "total_consumed_qty": round(month_consumed_qty, 2),
                    "total_wasted_qty": round(month_wasted_qty, 2),
                    "active_remaining_qty": round(active_remaining_qty, 2),
                    "active_lot_count": 1,
                    "sold_out_lot_count": 0,
                    "expired_lot_count": 1 if month_wasted_qty > 0 else 0,
                }
            )

        materialized_rows.sort(
            key=lambda row: (
                -self._safe_float(row.get("active_remaining_qty")),
                -self._safe_float(row.get("total_wasted_qty")),
                str(row.get("item_nm") or ""),
            )
        )
        total = len(materialized_rows)
        return materialized_rows[offset : offset + page_size], total

    def get_shelf_life_days_map(
        self,
        *,
        item_codes: list[str] | None = None,
        item_names: list[str] | None = None,
    ) -> dict[str, int]:
        if not self.engine or not has_table(self.engine, "raw_product_shelf_life"):
            return {}

        normalized_codes = [str(code).strip() for code in (item_codes or []) if str(code).strip()]
        normalized_names = [str(name).strip() for name in (item_names or []) if str(name).strip()]
        if not normalized_codes and not normalized_names:
            return {}

        where_clauses: list[str] = []
        params: dict[str, object] = {}
        if normalized_codes:
            where_clauses.append("NULLIF(TRIM(CAST(item_cd AS TEXT)), '') = ANY(:item_codes)")
            params["item_codes"] = normalized_codes
        if normalized_names:
            where_clauses.append("NULLIF(TRIM(CAST(item_nm AS TEXT)), '') = ANY(:item_names)")
            params["item_names"] = normalized_names

        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            f"""
                            SELECT
                                NULLIF(TRIM(CAST(item_cd AS TEXT)), '') AS item_cd,
                                NULLIF(TRIM(CAST(item_nm AS TEXT)), '') AS item_nm,
                                NULLIF(TRIM(CAST(shelf_life_days AS TEXT)), '') AS shelf_life_days
                            FROM raw_product_shelf_life
                            WHERE {" OR ".join(where_clauses)}
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError as exc:
            logger.warning("get_shelf_life_days_map query failed: error=%s", exc)
            return {}

        shelf_life_map: dict[str, int] = {}
        for row in rows:
            shelf_life_days = self._safe_int(row.get("shelf_life_days"))
            if shelf_life_days < 0:
                continue

            item_cd = str(row.get("item_cd") or "").strip()
            item_nm = str(row.get("item_nm") or "").strip()
            if item_cd and item_cd not in shelf_life_map:
                shelf_life_map[item_cd] = shelf_life_days
            if item_nm and item_nm not in shelf_life_map:
                shelf_life_map[item_nm] = shelf_life_days
        return shelf_life_map

    @staticmethod
    def _safe_non_negative_int(value: object) -> int:
        return max(0, ProductionRepository._safe_int(value))

    @staticmethod
    def _safe_float(value: object) -> float:
        if value in (None, ""):
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _normalize_item_key(item_cd: object, item_nm: object) -> str:
        item_cd_text = str(item_cd or "").strip()
        item_nm_text = str(item_nm or "").strip()
        return item_cd_text or item_nm_text

    @staticmethod
    def _normalize_menu_name_key(value: object) -> str:
        normalized = unicodedata.normalize("NFKC", str(value or ""))
        normalized = normalized.replace("\xa0", " ").strip()
        normalized = re.sub(r"^\[JBOD\]\s*", "", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\s+", "", normalized)
        return normalized.lower()

    @staticmethod
    def _expand_item_keys(item_cd: object, item_nm: object) -> set[str]:
        keys = {str(item_cd or "").strip(), str(item_nm or "").strip()}
        return {key for key in keys if key}

    @staticmethod
    def _parse_yyyymmdd(value: str) -> date_type:
        return datetime.strptime(str(value), "%Y%m%d").date()

    @staticmethod
    def _assumed_waste_shelf_life_days(item_nm: str) -> int:
        return 1 if str(item_nm or "").strip() else 1

    @classmethod
    def _compute_expiry_waste_rows(
        cls,
        *,
        production_rows: list[dict[str, object]],
        sales_rows: list[dict[str, object]],
        unit_price_map: dict[str, float],
        shelf_life_map: dict[str, int],
        date_from: str,
        date_to: str,
    ) -> list[dict[str, object]]:
        target_start = cls._parse_yyyymmdd(date_from)
        target_end = cls._parse_yyyymmdd(date_to)

        production_by_day: dict[date_type, list[dict[str, object]]] = defaultdict(list)
        sales_by_day: dict[date_type, list[dict[str, object]]] = defaultdict(list)
        item_name_by_key: dict[str, str] = {}
        item_cd_by_key: dict[str, str] = {}

        all_dates: set[date_type] = {target_start, target_end}

        for row in production_rows:
            prod_date_text = str(row.get("prod_dt") or "").strip()
            if not prod_date_text:
                continue
            prod_date = cls._parse_yyyymmdd(prod_date_text)
            item_key = cls._normalize_item_key(row.get("item_cd"), row.get("item_nm"))
            if not item_key:
                continue
            qty = cls._safe_float(row.get("produced_qty"))
            if qty <= 0:
                continue
            item_nm = str(row.get("item_nm") or item_key).strip()
            item_cd = str(row.get("item_cd") or "").strip()
            item_name_by_key[item_key] = item_nm
            item_cd_by_key[item_key] = item_cd or item_key
            shelf_life_days = shelf_life_map.get(item_cd) or shelf_life_map.get(item_nm)
            if shelf_life_days is None:
                shelf_life_days = cls._assumed_waste_shelf_life_days(item_nm)
            production_by_day[prod_date].append(
                {
                    "item_key": item_key,
                    "item_cd": item_cd or item_key,
                    "item_nm": item_nm,
                    "produced_qty": qty,
                    "shelf_life_days": max(int(shelf_life_days), 0),
                }
            )
            all_dates.add(prod_date)

        for row in sales_rows:
            sale_date_text = str(row.get("sale_dt") or "").strip()
            if not sale_date_text:
                continue
            sale_date = cls._parse_yyyymmdd(sale_date_text)
            item_key = cls._normalize_item_key(row.get("item_cd"), row.get("item_nm"))
            if not item_key:
                continue
            qty = cls._safe_float(row.get("sale_qty"))
            if qty <= 0:
                continue
            item_name_by_key.setdefault(item_key, str(row.get("item_nm") or item_key).strip())
            item_cd_by_key.setdefault(item_key, str(row.get("item_cd") or "").strip() or item_key)
            sales_by_day[sale_date].append({"item_key": item_key, "sale_qty": qty})
            all_dates.add(sale_date)

        active_lots_by_item: dict[str, list[dict[str, object]]] = defaultdict(list)
        waste_qty_by_item: dict[str, float] = defaultdict(float)

        current_date = min(all_dates)
        while current_date <= target_end:
            for prod in production_by_day.get(current_date, []):
                shelf_life_days = int(prod["shelf_life_days"])
                if shelf_life_days <= 0:
                    expiry_date = None
                else:
                    expiry_date = current_date + timedelta(days=shelf_life_days - 1)
                active_lots_by_item[str(prod["item_key"])].append(
                    {
                        "item_cd": prod["item_cd"],
                        "item_nm": prod["item_nm"],
                        "remaining_qty": float(prod["produced_qty"]),
                        "expiry_date": expiry_date,
                    }
                )

            for sale in sales_by_day.get(current_date, []):
                item_key = str(sale["item_key"])
                remaining_sale = float(sale["sale_qty"])
                lots = active_lots_by_item.get(item_key, [])
                for lot in lots:
                    if remaining_sale <= 0:
                        break
                    lot_remaining = float(lot.get("remaining_qty") or 0.0)
                    if lot_remaining <= 0:
                        continue
                    consumed = min(lot_remaining, remaining_sale)
                    lot["remaining_qty"] = lot_remaining - consumed
                    remaining_sale -= consumed

            for item_key, lots in active_lots_by_item.items():
                kept_lots: list[dict[str, object]] = []
                for lot in lots:
                    expiry_date = lot.get("expiry_date")
                    remaining_qty = float(lot.get("remaining_qty") or 0.0)
                    if expiry_date is not None and expiry_date == current_date:
                        if remaining_qty > 0 and target_start <= current_date <= target_end:
                            waste_qty_by_item[item_key] += remaining_qty
                        continue
                    kept_lots.append(lot)
                active_lots_by_item[item_key] = kept_lots

            current_date += timedelta(days=1)

        rows: list[dict[str, object]] = []
        for item_key, total_waste_qty in waste_qty_by_item.items():
            if total_waste_qty <= 0:
                continue
            item_nm = item_name_by_key.get(item_key, item_key)
            item_cd = item_cd_by_key.get(item_key, item_key)
            normalized_name_key = cls._normalize_menu_name_key(item_nm)
            avg_cost = cls._safe_float(
                unit_price_map.get(item_cd)
                or unit_price_map.get(item_nm)
                or unit_price_map.get(normalized_name_key)
            )
            rows.append(
                {
                    "item_cd": item_cd,
                    "item_nm": item_nm,
                    "total_waste_qty": round(total_waste_qty, 2),
                    "total_waste_amount": round(total_waste_qty * avg_cost, 2),
                    "avg_cost": avg_cost,
                }
            )

        rows.sort(key=lambda row: cls._safe_float(row.get("total_waste_qty")), reverse=True)
        return rows

    @staticmethod
    def _normalize_tmzon_hour(value: object) -> int | None:
        text_value = str(value or "").strip()
        if not text_value:
            return None
        digits = "".join(ch for ch in text_value if ch.isdigit())
        if not digits:
            return None
        hour = int(digits)
        return hour if 0 <= hour <= 23 else None

    @classmethod
    def _resolve_business_hours(
        cls,
        operating_hours: dict[str, int] | None = None,
    ) -> tuple[int, ...]:
        if not operating_hours:
            return cls._BUSINESS_HOURS

        open_hour = cls._normalize_tmzon_hour(operating_hours.get("open_hour"))
        close_hour = cls._normalize_tmzon_hour(operating_hours.get("close_hour"))
        if open_hour is None or close_hour is None or close_hour < open_hour:
            return cls._BUSINESS_HOURS

        return tuple(range(open_hour, close_hour + 1))

    @classmethod
    def _derive_stock_rate(cls, stock_qty: float, sale_qty: float) -> float:
        if sale_qty > 0:
            return round(stock_qty / sale_qty, 4)
        if stock_qty < 0:
            return -1.0
        if stock_qty > 0:
            return 1.0
        return 0.0

    @classmethod
    def _build_inventory_metric_row(cls, row: dict[str, object], dr: int) -> dict[str, object]:
        item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
        item_nm = str(row.get("item_nm") or item_cd).strip()
        stock_qty = cls._safe_float(row.get("stock_qty"))
        sale_qty = cls._safe_float(row.get("sale_qty"))
        orderable_qty = max(stock_qty + sale_qty, 0.0)
        return {
            "masked_stor_cd": row.get("masked_stor_cd"),
            "prc_dt": row.get("stock_dt"),
            "item_cd": item_cd,
            "item_nm": item_nm,
            "ord_avg": orderable_qty,
            "sal_avg": sale_qty,
            "stk_avg": stock_qty,
            "stk_rt": cls._derive_stock_rate(stock_qty, sale_qty),
            "dr": dr,
        }

    @classmethod
    def _infer_stockout_from_hourly_sales(
        cls,
        *,
        inventory_rows: list[dict[str, object]],
        hourly_rows: list[dict[str, object]],
        operating_hours: dict[str, int] | None = None,
    ) -> dict[str, dict[str, object]]:
        business_hours = cls._resolve_business_hours(operating_hours)
        tracked: dict[str, dict[str, object]] = {}
        for row in inventory_rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            if not item_cd:
                continue
            tracked[item_cd] = {
                "item_cd": item_cd,
                "item_nm": str(row.get("item_nm") or item_cd).strip(),
                "hourly_qty": {hour: 0.0 for hour in business_hours},
            }

        for row in hourly_rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            if not item_cd:
                continue
            bucket = tracked.setdefault(
                item_cd,
                {
                    "item_cd": item_cd,
                    "item_nm": str(row.get("item_nm") or item_cd).strip(),
                    "hourly_qty": {hour: 0.0 for hour in business_hours},
                },
            )
            hour = cls._normalize_tmzon_hour(row.get("tmzon_div"))
            if hour is None or hour not in business_hours:
                continue
            hourly_qty = bucket["hourly_qty"]
            if isinstance(hourly_qty, dict):
                hourly_qty[hour] = float(hourly_qty.get(hour, 0.0)) + cls._safe_float(
                    row.get("sale_qty")
                )

        inferred: dict[str, dict[str, object]] = {}
        for item_cd, bucket in tracked.items():
            hourly_qty = bucket.get("hourly_qty")
            sales_by_hour = hourly_qty if isinstance(hourly_qty, dict) else {}
            last_positive_sale_hour: int | None = None
            for hour in business_hours:
                qty = cls._safe_float(sales_by_hour.get(hour, 0.0))
                if qty > 0:
                    last_positive_sale_hour = hour

            stockout_hour: int | None = None
            if last_positive_sale_hour is not None:
                trailing_zero_hours = business_hours[-1] - last_positive_sale_hour
                if trailing_zero_hours >= cls._STOCKOUT_ZERO_SALES_WINDOW:
                    stockout_hour = last_positive_sale_hour + 1

            inferred[item_cd] = {
                "item_cd": item_cd,
                "item_nm": bucket.get("item_nm") or item_cd,
                "is_stockout": stockout_hour is not None,
                "stockout_hour": stockout_hour,
            }

        return inferred

    @staticmethod
    def _summarize_inventory_rows(rows: list[dict[str, object]]) -> dict[str, object]:
        shortage_count = 0
        excess_count = 0
        normal_count = 0
        stock_rates: list[float] = []
        for row in rows:
            stock_rate = ProductionRepository._safe_float(row.get("stk_rt"))
            is_stockout = bool(row.get("is_stockout"))
            stock_rates.append(stock_rate)
            if stock_rate < 0 or is_stockout:
                shortage_count += 1
            elif stock_rate >= 0.35:
                excess_count += 1
            else:
                normal_count += 1

        avg_stock_rate = sum(stock_rates) / len(stock_rates) if stock_rates else None
        return {
            "shortage_count": shortage_count,
            "excess_count": excess_count,
            "normal_count": normal_count,
            "avg_stock_rate": avg_stock_rate,
        }

    def _fetch_recent_inventory_rows(
        self, store_id: str, rank_limit: int = 2
    ) -> list[dict[str, object]]:
        if not self.engine or not has_table(self.engine, "raw_inventory_extract"):
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH latest_dates AS (
                                SELECT
                                    stock_dt,
                                    DENSE_RANK() OVER (ORDER BY stock_dt DESC) AS dr
                                FROM (
                                    SELECT DISTINCT stock_dt
                                    FROM raw_inventory_extract
                                    WHERE masked_stor_cd = :store_id
                                      AND stock_dt IS NOT NULL
                                ) dates
                            )
                            SELECT
                                r.masked_stor_cd,
                                d.dr,
                                r.stock_dt,
                                COALESCE(NULLIF(TRIM(r.item_cd), ''), NULLIF(TRIM(r.item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(r.item_nm), ''), NULLIF(TRIM(r.item_cd), '')) AS item_nm,
                                SUM(COALESCE(NULLIF(TRIM(r.stock_qty), '')::numeric, 0)) AS stock_qty,
                                SUM(COALESCE(NULLIF(TRIM(r.sale_qty), '')::numeric, 0)) AS sale_qty
                            FROM raw_inventory_extract r
                            JOIN latest_dates d ON r.stock_dt = d.stock_dt
                            WHERE r.masked_stor_cd = :store_id
                              AND d.dr <= :rank_limit
                            GROUP BY
                                r.masked_stor_cd,
                                d.dr,
                                r.stock_dt,
                                COALESCE(NULLIF(TRIM(r.item_cd), ''), NULLIF(TRIM(r.item_nm), '')),
                                COALESCE(NULLIF(TRIM(r.item_nm), ''), NULLIF(TRIM(r.item_cd), ''))
                            ORDER BY item_nm, d.dr
                            """
                        ),
                        {"store_id": store_id, "rank_limit": rank_limit},
                    )
                    .mappings()
                    .all()
                )
            return [dict(row) for row in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_recent_inventory_rows query failed: store_id=%s error=%s",
                store_id,
                exc,
            )
            return []

    def _fetch_latest_hourly_sales_rows(
        self, store_id: str
    ) -> tuple[str | None, list[dict[str, object]]]:
        if not self.engine or not has_table(self.engine, "core_hourly_item_sales"):
            return None, []
        try:
            with self.engine.connect() as conn:
                latest_sale_dt = conn.execute(
                    text(
                        """
                        SELECT MAX(sale_dt)
                        FROM core_hourly_item_sales
                        WHERE masked_stor_cd = :store_id
                        """
                    ),
                    {"store_id": store_id},
                ).scalar_one_or_none()
                if not latest_sale_dt:
                    return None, []
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')) AS item_nm,
                                tmzon_div,
                                SUM(COALESCE(sale_qty, 0)) AS sale_qty
                            FROM core_hourly_item_sales
                            WHERE masked_stor_cd = :store_id
                              AND sale_dt = :sale_dt
                            GROUP BY
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')),
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')),
                                tmzon_div
                            """
                        ),
                        {"store_id": store_id, "sale_dt": latest_sale_dt},
                    )
                    .mappings()
                    .all()
                )
            return str(latest_sale_dt), [dict(row) for row in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_latest_hourly_sales_rows query failed: store_id=%s error=%s",
                store_id,
                exc,
            )
            return None, []

    def _fetch_inventory_rows_for_date(
        self, store_id: str, sale_date: str
    ) -> list[dict[str, object]]:
        if not self.engine or not has_table(self.engine, "raw_inventory_extract"):
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                masked_stor_cd,
                                stock_dt,
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')) AS item_nm,
                                SUM(COALESCE(NULLIF(TRIM(stock_qty), '')::numeric, 0)) AS stock_qty,
                                SUM(COALESCE(NULLIF(TRIM(sale_qty), '')::numeric, 0)) AS sale_qty
                            FROM raw_inventory_extract
                            WHERE masked_stor_cd = :store_id
                              AND stock_dt = :sale_date
                            GROUP BY
                                masked_stor_cd,
                                stock_dt,
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')),
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), ''))
                            """
                        ),
                        {"store_id": store_id, "sale_date": sale_date},
                    )
                    .mappings()
                    .all()
                )
            return [dict(row) for row in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_inventory_rows_for_date query failed: store_id=%s sale_date=%s error=%s",
                store_id,
                sale_date,
                exc,
            )
            return []

    def _fetch_hourly_sales_rows_for_date(
        self, store_id: str, sale_date: str
    ) -> list[dict[str, object]]:
        if not self.engine or not has_table(self.engine, "core_hourly_item_sales"):
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')) AS item_nm,
                                tmzon_div,
                                SUM(COALESCE(sale_qty, 0)) AS sale_qty
                            FROM core_hourly_item_sales
                            WHERE masked_stor_cd = :store_id
                              AND sale_dt = :sale_date
                            GROUP BY
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')),
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')),
                                tmzon_div
                            """
                        ),
                        {"store_id": store_id, "sale_date": sale_date},
                    )
                    .mappings()
                    .all()
                )
            return [dict(row) for row in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_hourly_sales_rows_for_date query failed: store_id=%s sale_date=%s error=%s",
                store_id,
                sale_date,
                exc,
            )
            return []

    def _fetch_recorded_stockout_rows_for_date(
        self, store_id: str, sale_date: str
    ) -> list[dict[str, object]]:
        if not self.engine or not has_table(self.engine, "core_stockout_time"):
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                item_cd,
                                item_nm,
                                is_stockout,
                                stockout_hour
                            FROM core_stockout_time
                            WHERE masked_stor_cd = :store_id
                              AND prc_dt = :sale_date
                            """
                        ),
                        {"store_id": store_id, "sale_date": sale_date},
                    )
                    .mappings()
                    .all()
                )
            return [dict(row) for row in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_recorded_stockout_rows_for_date query failed: store_id=%s sale_date=%s error=%s",
                store_id,
                sale_date,
                exc,
            )
            return []

    def _fetch_store_operating_hours(self, store_id: str) -> dict[str, int] | None:
        if not self.engine or not has_table(self.engine, "store_operating_hours"):
            return None
        try:
            with self.engine.connect() as conn:
                row = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                OPEN_HOUR AS open_hour,
                                CLOSE_HOUR AS close_hour
                            FROM store_operating_hours
                            WHERE MASKED_STOR_CD = :store_id
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .first()
                )
            return dict(row) if row else None
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_store_operating_hours query failed: store_id=%s error=%s",
                store_id,
                exc,
            )
            return None

    def list_stockout_event_targets(
        self,
        *,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[tuple[str, str]]:
        if not self.engine:
            return []

        targets: set[tuple[str, str]] = set()
        source_specs: list[tuple[str, str]] = []
        if has_table(self.engine, "raw_inventory_extract"):
            source_specs.append(("raw_inventory_extract", "stock_dt"))
        if has_table(self.engine, "core_hourly_item_sales"):
            source_specs.append(("core_hourly_item_sales", "sale_dt"))
        if has_table(self.engine, "core_stockout_time"):
            source_specs.append(("core_stockout_time", "prc_dt"))

        if not source_specs:
            return []

        try:
            with self.engine.connect() as conn:
                for table_name, date_column in source_specs:
                    clauses = [f"{date_column} IS NOT NULL"]
                    params: dict[str, object] = {}
                    if store_id:
                        clauses.append("masked_stor_cd = :store_id")
                        params["store_id"] = store_id
                    if date_from:
                        clauses.append(f"{date_column} >= :date_from")
                        params["date_from"] = date_from
                    if date_to:
                        clauses.append(f"{date_column} <= :date_to")
                        params["date_to"] = date_to
                    rows = (
                        conn.execute(
                            text(
                                f"""
                                SELECT DISTINCT masked_stor_cd, {date_column} AS sale_dt
                                FROM {table_name}
                                WHERE {' AND '.join(clauses)}
                                """
                            ),
                            params,
                        )
                        .mappings()
                        .all()
                    )
                    for row in rows:
                        scoped_store_id = str(row.get("masked_stor_cd") or "").strip()
                        scoped_sale_dt = str(row.get("sale_dt") or "").strip()
                        if scoped_store_id and scoped_sale_dt:
                            targets.add((scoped_store_id, scoped_sale_dt))
        except SQLAlchemyError as exc:
            logger.warning(
                "list_stockout_event_targets query failed: store_id=%s date_from=%s date_to=%s error=%s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            return []

        return sorted(targets, key=lambda target: (target[0], target[1]))

    def list_inferred_stockout_events(self, store_id: str, sale_date: str) -> list[dict[str, object]]:
        operating_hours = self._fetch_store_operating_hours(store_id=store_id)
        business_hours = self._resolve_business_hours(operating_hours)
        open_hour = business_hours[0] if business_hours else None
        close_hour = business_hours[-1] if business_hours else None

        recorded_rows = self._fetch_recorded_stockout_rows_for_date(store_id=store_id, sale_date=sale_date)
        recorded_item_codes: set[str] = set()
        events: list[dict[str, object]] = []

        for row in recorded_rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            if not item_cd:
                continue
            recorded_item_codes.add(item_cd)
            if not bool(row.get("is_stockout")):
                continue
            stockout_hour = self._normalize_tmzon_hour(row.get("stockout_hour"))
            if stockout_hour is None:
                continue
            item_nm = str(row.get("item_nm") or item_cd).strip()
            events.append(
                {
                    "masked_stor_cd": store_id,
                    "sale_dt": sale_date,
                    "item_cd": item_cd,
                    "item_nm": item_nm,
                    "is_stockout": True,
                    "stockout_hour": stockout_hour,
                    "rule_type": "raw_stockout_time",
                    "source_table": "core_stockout_time",
                    "open_hour": open_hour,
                    "close_hour": close_hour,
                    "zero_sales_window": None,
                    "evidence_start_hour": stockout_hour,
                    "evidence_end_hour": stockout_hour,
                }
            )

        inventory_rows = self._fetch_inventory_rows_for_date(store_id=store_id, sale_date=sale_date)
        hourly_rows = self._fetch_hourly_sales_rows_for_date(store_id=store_id, sale_date=sale_date)
        inferred_map = self._infer_stockout_from_hourly_sales(
            inventory_rows=inventory_rows,
            hourly_rows=hourly_rows,
            operating_hours=operating_hours,
        )

        for item_cd, row in inferred_map.items():
            if item_cd in recorded_item_codes:
                continue
            if not bool(row.get("is_stockout")):
                continue

            stockout_hour = self._normalize_tmzon_hour(row.get("stockout_hour"))
            if stockout_hour is None:
                continue

            item_nm = str(row.get("item_nm") or item_cd).strip()
            evidence_end_hour = min(
                stockout_hour + self._STOCKOUT_ZERO_SALES_WINDOW - 1,
                close_hour if close_hour is not None else stockout_hour + self._STOCKOUT_ZERO_SALES_WINDOW - 1,
            )
            events.append(
                {
                    "masked_stor_cd": store_id,
                    "sale_dt": sale_date,
                    "item_cd": item_cd,
                    "item_nm": item_nm,
                    "is_stockout": True,
                    "stockout_hour": stockout_hour,
                    "rule_type": "hourly_zero_sales_3h",
                    "source_table": "core_hourly_item_sales/raw_inventory_extract",
                    "open_hour": open_hour,
                    "close_hour": close_hour,
                    "zero_sales_window": self._STOCKOUT_ZERO_SALES_WINDOW,
                    "evidence_start_hour": stockout_hour,
                    "evidence_end_hour": evidence_end_hour,
                }
            )

        events.sort(key=lambda event: (str(event["masked_stor_cd"]), str(event["sale_dt"]), int(event["stockout_hour"]), str(event["item_cd"])))
        return events

    def _get_stock_rate_recent_rows_fallback(self, store_id: str) -> list[dict]:
        inventory_rows = self._fetch_recent_inventory_rows(store_id=store_id, rank_limit=2)
        if not inventory_rows:
            return []
        return [
            self._build_inventory_metric_row(row, int(row.get("dr") or 1))
            for row in inventory_rows
        ]

    def _get_stockout_latest_rows_fallback(self, store_id: str) -> list[dict]:
        inventory_rows = [
            row
            for row in self._fetch_recent_inventory_rows(store_id=store_id, rank_limit=1)
            if int(row.get("dr") or 0) == 1
        ]
        if not inventory_rows:
            return []
        _, hourly_rows = self._fetch_latest_hourly_sales_rows(store_id=store_id)
        operating_hours = self._fetch_store_operating_hours(store_id=store_id)
        stockout_map = self._infer_stockout_from_hourly_sales(
            inventory_rows=inventory_rows,
            hourly_rows=hourly_rows,
            operating_hours=operating_hours,
        )
        return list(stockout_map.values())

    def _get_inventory_status_fallback(
        self,
        store_id: str,
        page: int,
        page_size: int,
        status_filters: list[str] | None = None,
    ) -> tuple[list[dict], int, dict]:
        offset = max(0, (page - 1) * page_size)
        inventory_rows = [
            row
            for row in self._fetch_recent_inventory_rows(store_id=store_id, rank_limit=1)
            if int(row.get("dr") or 0) == 1
        ]
        if not inventory_rows:
            return [], 0, {}

        _, hourly_rows = self._fetch_latest_hourly_sales_rows(store_id=store_id)
        operating_hours = self._fetch_store_operating_hours(store_id=store_id)
        stockout_map = self._infer_stockout_from_hourly_sales(
            inventory_rows=inventory_rows,
            hourly_rows=hourly_rows,
            operating_hours=operating_hours,
        )

        rows: list[dict[str, object]] = []
        for row in inventory_rows:
            metric_row = self._build_inventory_metric_row(row, 1)
            stockout_row = stockout_map.get(str(metric_row["item_cd"]), {})
            metric_row["is_stockout"] = bool(
                stockout_row.get("is_stockout") or self._safe_float(metric_row.get("stk_avg")) < 0
            )
            metric_row["stockout_hour"] = stockout_row.get("stockout_hour")
            rows.append(metric_row)

        if status_filters:
            rows = [row for row in rows if str(row.get("status")) in status_filters]

        rows.sort(key=lambda item: (str(item.get("item_cd") or ""), str(item.get("item_nm") or "")))
        summary_metrics = self._summarize_inventory_rows(rows)
        return rows[offset : offset + page_size], len(rows), summary_metrics

    @staticmethod
    @staticmethod
    def _build_inventory_status_filter_clause(
        status_filters: list[str] | None,
        column_name: str = "status",
    ) -> tuple[str, dict[str, str]]:
        if not status_filters:
            return "", {}

        code_to_label = {
            "shortage": "????",
            "excess": "???",
            "normal": "???",
        }
        normalized_values = [code_to_label.get(value, value) for value in status_filters]
        filter_params = {
            f"status_filter_{index}": value for index, value in enumerate(normalized_values)
        }
        filter_clause = " AND (" + " OR ".join(
            f"{column_name} = :status_filter_{index}" for index, _ in enumerate(normalized_values)
        ) + ")"
        return filter_clause, filter_params
    @staticmethod
    @staticmethod
    def _build_inventory_mart_filter_clause(
        status_filters: list[str] | None,
        *,
        stock_rate_column: str = "stock_rate",
        is_stockout_column: str = "is_stockout",
    ) -> str:
        if not status_filters:
            return ""

        clauses: list[str] = []
        for value in status_filters:
            code = str(value).strip().lower()
            if code == "shortage":
                clauses.append(
                    f"(COALESCE({is_stockout_column}, FALSE) = TRUE OR COALESCE({stock_rate_column}, 0) < 0)"
                )
            elif code == "excess":
                clauses.append(
                    f"(COALESCE({is_stockout_column}, FALSE) = FALSE AND COALESCE({stock_rate_column}, 0) >= 0.35)"
                )
            elif code == "normal":
                clauses.append(
                    f"(COALESCE({is_stockout_column}, FALSE) = FALSE AND COALESCE({stock_rate_column}, 0) >= 0 AND COALESCE({stock_rate_column}, 0) < 0.35)"
                )
        if not clauses:
            return ""
        return " AND (" + " OR ".join(clauses) + ")"
    @staticmethod
    def _clamp_recommended_qty(current: int, forecast: int, candidate: int) -> int:
        if forecast <= 0:
            return 0
        lower_bound = max(4, int(round(forecast * 0.2)))
        upper_bound = max(8, int(round(forecast * 1.5)))
        return max(lower_bound, min(candidate, upper_bound))

    @staticmethod
    def _parse_date_str(value: str) -> date_type | None:
        """YYYYMMDD 또는 YYYY-MM-DD 형식 텍스트를 date 객체로 변환."""
        s = value.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    def _fetch_metric_map(
        self,
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        metric_candidates: tuple[str, ...],
        store_id: str | None = None,
        window_days: int = 0,
        reference_date: str | None = None,
    ) -> dict[str, dict[str, object]]:
        """품목별 지표 집계를 반환.

        window_days=0이면 기준 날짜 단일 기준 합산,
        window_days>0이면 기준 날짜 기준 window_days일 동안 발생한
        날짜별 합산을 생산 발생일 수로 나눈 일평균을 반환한다.
        """
        columns = self._table_columns(relation)
        date_column = self._pick_column(columns, date_candidates)
        item_name_column = self._pick_column(columns, item_name_candidates)
        item_code_column = self._pick_column(columns, item_code_candidates)
        metric_column = self._pick_column(columns, metric_candidates)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        if not self.engine or not date_column or not item_name_column or not metric_column:
            logger.debug(
                "_fetch_metric_map 컬럼 해석 실패: relation=%s date=%s name=%s metric=%s",
                relation,
                date_column,
                item_name_column,
                metric_column,
            )
            return {}

        item_name_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_name_column} AS TEXT)), ''), NULLIF(TRIM(CAST({item_code_column} AS TEXT)), ''))"
            if item_code_column
            else f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')"
        )
        item_code_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_code_column} AS TEXT)), ''), NULLIF(TRIM(CAST({item_name_column} AS TEXT)), ''))"
            if item_code_column
            else f"NULLIF(TRIM(CAST({item_code_column} AS TEXT)), '')"
        )
        metric_expr = f"COALESCE(NULLIF(TRIM(CAST({metric_column} AS TEXT)), '')::numeric, 0)"

        store_filter = (
            f"AND CAST({store_column} AS TEXT) = :store_id" if store_id and store_column else ""
        )
        params_date: dict = {"store_id": store_id} if store_id else {}
        params_row: dict = {"store_id": store_id} if store_id else {}

        try:
            with self.engine.connect() as connection:
                latest_date: str | None = None
                if reference_date:
                    reference_dt = self._parse_date_str(reference_date)
                    if reference_dt is not None:
                        latest_date = reference_dt.strftime("%Y%m%d")
                if latest_date is None:
                    latest_date = connection.execute(
                        text(
                            f"""
                            SELECT DISTINCT CAST({date_column} AS TEXT) AS date_value
                            FROM {relation}
                            WHERE NULLIF(TRIM(CAST({date_column} AS TEXT)), '') IS NOT NULL
                            {store_filter}
                            ORDER BY date_value DESC
                            LIMIT 1
                            """
                        ),
                        params_date,
                    ).scalar_one_or_none()
                if not latest_date:
                    logger.debug("_fetch_metric_map 기준 날짜 없음: relation=%s store_id=%s", relation, store_id)
                    logger.debug(
                        "_fetch_metric_map 최신 날짜 없음: relation=%s store_id=%s",
                        relation,
                        store_id,
                    )
                    return {}

                if window_days > 0:
                    # 4주 평균 모드: window 내 날짜별 합계 → 생산 발생일 기준 평균
                    latest_dt = self._parse_date_str(str(latest_date))
                    if not latest_dt:
                        logger.warning(
                            "_fetch_metric_map: 날짜 파싱 실패 latest_date=%s relation=%s",
                            latest_date,
                            relation,
                        )
                        return {}
                    min_date_str = (latest_dt - timedelta(days=window_days - 1)).strftime("%Y%m%d")
                    max_date_str = latest_dt.strftime("%Y%m%d")

                    rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                {item_name_expr} AS item_name,
                                {item_code_expr} AS item_code,
                                CAST({date_column} AS TEXT) AS date_val,
                                {metric_expr} AS metric_value
                            FROM {relation}
                            WHERE CAST({date_column} AS TEXT) BETWEEN :min_date AND :max_date
                            {store_filter}
                            """
                            ),
                            {"min_date": min_date_str, "max_date": max_date_str, **params_row},
                        )
                        .mappings()
                        .all()
                    )
                else:
                    rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                {item_name_expr} AS item_name,
                                {item_code_expr} AS item_code,
                                {metric_expr} AS metric_value
                            FROM {relation}
                            WHERE CAST({date_column} AS TEXT) = :date_value
                            {store_filter}
                            """
                            ),
                            {"date_value": str(latest_date), **params_row},
                        )
                        .mappings()
                        .all()
                    )
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_metric_map 쿼리 실패: relation=%s store_id=%s error=%s",
                relation,
                store_id,
                exc,
            )
            return {}

        metric_map: dict[str, dict[str, object]] = {}

        if window_days > 0:
            # 1. 날짜별 합산
            daily_sums: dict[str, dict[str, object]] = {}
            for row in rows:
                item_name = (
                    str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                )
                item_code = (
                    str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
                )
                key = item_code or item_name
                if not key:
                    continue
                date_val = str(row.get("date_val", ""))
                bucket = daily_sums.setdefault(
                    key,
                    {
                        "item_cd": item_code or key,
                        "item_nm": item_name or item_code or key,
                        "dates": set(),
                        "total_qty": 0,
                    },
                )
                bucket["item_cd"] = item_code or bucket["item_cd"]
                bucket["item_nm"] = item_name or bucket["item_nm"]
                bucket["dates"].add(date_val)  # type: ignore[union-attr]
                bucket["total_qty"] = int(bucket["total_qty"]) + self._safe_int(row["metric_value"])

            # 2. 발생일 기준 일평균
            for key, bucket in daily_sums.items():
                n_days = max(1, window_days)
                metric_map[key] = {
                    "item_cd": bucket["item_cd"],
                    "item_nm": bucket["item_nm"],
                    "qty": int(round(int(bucket["total_qty"]) / n_days)),
                }
        else:
            for row in rows:
                item_name = (
                    str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                )
                item_code = (
                    str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
                )
                key = item_code or item_name
                if not key:
                    continue
                bucket = metric_map.setdefault(
                    key,
                    {
                        "item_cd": item_code or key,
                        "item_nm": item_name or item_code or key,
                        "qty": 0,
                    },
                )
                bucket["qty"] = int(bucket["qty"]) + self._safe_int(row["metric_value"])

        return metric_map

    @staticmethod
    def _scale_down_poc_qty(val: int) -> int:
        """POC 화면 표시를 위한 수량 보정."""
        if val <= 30:
            return val
        if val <= 100:
            return 20 + (val % 15)
        if val <= 500:
            return 25 + (val % 20)
        return 30 + (val % 20)

    @staticmethod
    def _resolve_item_identity(
        key: str,
        production: dict[str, object],
        secondary: dict[str, object],
        stock: dict[str, object],
        sale: dict[str, object],
        order_confirm: dict[str, object],
        hourly_sale: dict[str, object],
    ) -> tuple[str, str]:
        item_cd = str(
            production.get("item_cd")
            or stock.get("item_cd")
            or sale.get("item_cd")
            or secondary.get("item_cd")
            or key
        )
        item_nm = str(
            production.get("item_nm")
            or stock.get("item_nm")
            or sale.get("item_nm")
            or order_confirm.get("item_nm")
            or hourly_sale.get("item_nm")
            or secondary.get("item_nm")
            or key
        )
        return item_cd, item_nm

    def _build_ranked_row(
        self,
        key: str,
        production: dict[str, object],
        secondary: dict[str, object],
        stock: dict[str, object],
        sale: dict[str, object],
        order_confirm: dict[str, object],
        hourly_sale: dict[str, object],
    ) -> dict[str, object]:
        item_cd, item_nm = self._resolve_item_identity(
            key,
            production=production,
            secondary=secondary,
            stock=stock,
            sale=sale,
            order_confirm=order_confirm,
            hourly_sale=hourly_sale,
        )

        stock_qty = self._safe_non_negative_int(stock.get("qty")) if stock else 0
        production_qty = self._safe_non_negative_int(production.get("qty")) if production else 0
        secondary_qty = self._safe_non_negative_int(secondary.get("qty")) if secondary else 0
        sale_qty = self._safe_non_negative_int(sale.get("qty")) if sale else 0
        order_confirm_qty = (
            self._safe_non_negative_int(order_confirm.get("qty")) if order_confirm else 0
        )
        hourly_sale_qty = self._safe_non_negative_int(hourly_sale.get("qty")) if hourly_sale else 0

        stock_qty = self._scale_down_poc_qty(stock_qty)
        production_qty = self._scale_down_poc_qty(production_qty)
        secondary_qty = self._scale_down_poc_qty(secondary_qty)
        sale_qty = self._scale_down_poc_qty(sale_qty)
        order_confirm_qty = self._scale_down_poc_qty(order_confirm_qty)
        hourly_sale_qty = self._scale_down_poc_qty(hourly_sale_qty)

        current = stock_qty if stock else production_qty
        if not stock and current <= 0 and production_qty > 0:
            current = production_qty

        predicted_sales_1h = max(sale_qty, order_confirm_qty, hourly_sale_qty)
        if predicted_sales_1h <= 0 and production_qty > 0:
            predicted_sales_1h = min(production_qty, max(4, current + max(4, current // 2)))
        if predicted_sales_1h <= 0:
            predicted_sales_1h = max(0, current // 2)
        current = max(0, current)
        predicted_sales_1h = max(0, predicted_sales_1h)
        forecast = max(current - predicted_sales_1h, 0)

        order_pressure = order_confirm_qty >= max(1, int(round(predicted_sales_1h * 0.8)))
        velocity_pressure = hourly_sale_qty >= max(1, int(round(predicted_sales_1h * 0.8)))

        if predicted_sales_1h <= 0:
            status = "safe"
        elif current <= predicted_sales_1h or (
            order_pressure and current <= int(round(predicted_sales_1h * 1.1))
        ):
            status = "danger"
        elif current <= int(round(predicted_sales_1h * 1.5)) or velocity_pressure:
            status = "warning"
        else:
            status = "safe"
        if status == "safe":
            recommended = 0
        else:
            gap = max(predicted_sales_1h - current, 0)
            buffer_qty = max(4, int(round(predicted_sales_1h * 0.2)))
            recommended = self._clamp_recommended_qty(
                current, predicted_sales_1h, gap + buffer_qty
            )

        # 화면의 1차/2차 생산량은 최근 4주 평균 실적을 그대로 보여준다.
        # 추천 생산수량은 별도 recommended 필드에서 계산한다.
        prod1_qty = max(production_qty, 0)
        prod2_qty = max(secondary_qty, 0)

        risk_score = max(predicted_sales_1h - current, 0)
        return {
            "sku_id": item_cd,
            "name": item_nm,
            "current": current,
            "forecast": forecast,
            "predicted_sales_1h": predicted_sales_1h,
            "order_confirm_qty": order_confirm_qty,
            "hourly_sale_qty": hourly_sale_qty,
            "order_pressure": order_pressure,
            "velocity_pressure": velocity_pressure,
            "status": status,
            "depletion_time": "-",
            "recommended": recommended,
            "prod1": f"08:00 / {prod1_qty}개",
            "prod2": f"14:00 / {prod2_qty}개",
            "_risk_score": risk_score,
        }

    @staticmethod
    def _finalize_ranked_rows(ranked_rows: list[dict[str, object]]) -> list[dict]:
        status_priority = {"danger": 0, "warning": 1, "safe": 2}
        ranked_rows.sort(
            key=lambda row: (
                status_priority.get(str(row.get("status") or "safe"), 3),
                -int(row["_risk_score"]),
                -int(row["forecast"]),
                str(row["name"]),
            )
        )

        now = datetime.now()
        for row in ranked_rows:
            if row["status"] == "safe" or row["forecast"] <= 0:
                row["depletion_time"] = "-"
            else:
                # 실시간 기반: 현재고 / (일일판매량 / 8운영시간) = 소진까지 남은 시간
                hourly_rate = row["forecast"] / 8.0
                hours_until = min(23.0, row["current"] / max(hourly_rate, 0.1))
                depletion_dt = now + timedelta(hours=hours_until)
                row["depletion_time"] = depletion_dt.strftime("%H:%M")
            row.pop("_risk_score", None)

        return ranked_rows

    def _build_new_items(
        self,
        production_map: dict[str, dict[str, object]],
        secondary_map: dict[str, dict[str, object]],
        stock_map: dict[str, dict[str, object]],
        sale_map: dict[str, dict[str, object]],
        order_confirm_map: dict[str, dict[str, object]],
        hourly_sale_map: dict[str, dict[str, object]],
        active_keys: set[str] | None = None,
    ) -> list[dict]:
        combined_keys = (
            active_keys
            if active_keys is not None
            else (
                set(production_map)
                | set(secondary_map)
                | set(stock_map)
                | set(sale_map)
                | set(order_confirm_map)
                | set(hourly_sale_map)
            )
        )
        if not combined_keys:
            return []

        ranked_rows: list[dict[str, object]] = []
        for key in combined_keys:
            production = production_map.get(key, {})
            secondary = secondary_map.get(key, {})
            stock = stock_map.get(key, {})
            sale = sale_map.get(key, {})
            order_confirm = order_confirm_map.get(key, {})
            hourly_sale = hourly_sale_map.get(key, {})
            ranked_rows.append(
                self._build_ranked_row(
                    key,
                    production=production,
                    secondary=secondary,
                    stock=stock,
                    sale=sale,
                    order_confirm=order_confirm,
                    hourly_sale=hourly_sale,
                )
            )

        return self._finalize_ranked_rows(ranked_rows)

    @staticmethod
    def _normalize_business_date_text(value: str | None) -> str:
        if value:
            return value.replace("-", "").strip()
        return datetime.now(_KST).strftime("%Y%m%d")

    @classmethod
    def _resolve_reference_hour(cls, reference_datetime: datetime | None) -> int:
        base = reference_datetime.astimezone(_KST) if reference_datetime else datetime.now(_KST)
        hour = int(base.hour)
        return max(cls._BUSINESS_HOURS[0], min(22, hour))

    @staticmethod
    def _recent_same_weekday_dates(reference_date_text: str, weeks: int = 4) -> list[str]:
        reference_date = datetime.strptime(reference_date_text, "%Y%m%d").date()
        return [
            (reference_date - timedelta(days=7 * offset)).strftime("%Y%m%d")
            for offset in range(1, weeks + 1)
        ]

    @staticmethod
    def _build_item_filter_clause(
        *,
        params: dict[str, object],
        item_keys: set[str],
        code_column: str,
        name_column: str,
    ) -> str:
        if not item_keys:
            return ""
        placeholders: list[str] = []
        for index, item_key in enumerate(sorted(item_keys)):
            key_name = f"item_key_{index}"
            params[key_name] = item_key
            placeholders.append(f":{key_name}")
        placeholder_sql = ", ".join(placeholders)
        return (
            f" AND (CAST({code_column} AS TEXT) IN ({placeholder_sql})"
            f" OR CAST({name_column} AS TEXT) IN ({placeholder_sql}))"
        )

    def _enrich_items_with_historical_metrics(
        self,
        *,
        items: list[dict],
        store_id: str | None,
        business_date: str | None,
        reference_datetime: datetime | None,
    ) -> list[dict]:
        if not items or not self.engine or not store_id:
            return items
        if not has_table(self.engine, "raw_daily_store_item_tmzon") or not has_table(
            self.engine, "raw_production_extract"
        ):
            return items

        reference_date_text = self._normalize_business_date_text(
            business_date or (reference_datetime.astimezone(_KST).strftime("%Y-%m-%d") if reference_datetime else None)
        )
        historical_dates = self._recent_same_weekday_dates(reference_date_text, weeks=4)
        if not historical_dates:
            return items

        target_dates = historical_dates + [reference_date_text]
        date_from = min(target_dates)
        date_to = max(target_dates)
        reference_hour = self._resolve_reference_hour(reference_datetime)
        prior_hours = tuple(hour for hour in range(self._BUSINESS_HOURS[0], reference_hour))
        future_hours = tuple(range(reference_hour, 23))
        item_keys = {
            str(item.get("sku_id") or item.get("name") or "").strip()
            for item in items
            if str(item.get("sku_id") or item.get("name") or "").strip()
        }
        if not item_keys:
            return items

        sales_params: dict[str, object] = {
            "store_id": store_id,
            "date_from": date_from,
            "date_to": date_to,
        }
        production_params: dict[str, object] = {
            "store_id": store_id,
            "date_from": min(historical_dates),
            "date_to": max(historical_dates),
        }
        sales_item_filter = self._build_item_filter_clause(
            params=sales_params,
            item_keys=item_keys,
            code_column="item_cd",
            name_column="item_nm",
        )
        production_item_filter = self._build_item_filter_clause(
            params=production_params,
            item_keys=item_keys,
            code_column="item_cd",
            name_column="item_nm",
        )

        try:
            with self.engine.connect() as connection:
                sales_rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT
                                REPLACE(CAST(sale_dt AS TEXT), '-', '') AS sale_dt,
                                CAST(item_cd AS TEXT) AS item_cd,
                                CAST(item_nm AS TEXT) AS item_nm,
                                CAST(tmzon_div AS TEXT) AS tmzon_div,
                                COALESCE(NULLIF(TRIM(CAST(sale_qty AS TEXT)), '')::numeric, 0) AS sale_qty
                            FROM raw_daily_store_item_tmzon
                            WHERE CAST(masked_stor_cd AS TEXT) = :store_id
                              AND REPLACE(CAST(sale_dt AS TEXT), '-', '') BETWEEN :date_from AND :date_to
                              {sales_item_filter}
                            """
                        ),
                        sales_params,
                    )
                    .mappings()
                    .all()
                )
                production_rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT
                                REPLACE(CAST(prod_dt AS TEXT), '-', '') AS prod_dt,
                                CAST(item_cd AS TEXT) AS item_cd,
                                CAST(item_nm AS TEXT) AS item_nm,
                                COALESCE(NULLIF(TRIM(CAST(prod_qty AS TEXT)), '')::numeric, 0) AS prod_qty,
                                COALESCE(
                                    NULLIF(TRIM(CAST(prod_qty_2 AS TEXT)), '')::numeric,
                                    NULLIF(TRIM(CAST(reprod_qty AS TEXT)), '')::numeric,
                                    NULLIF(TRIM(CAST(prod_qty_3 AS TEXT)), '')::numeric,
                                    0
                                ) AS prod_qty_2,
                                COALESCE(NULLIF(TRIM(CAST(sale_prc AS TEXT)), '')::numeric, 0) AS sale_prc
                            FROM raw_production_extract
                            WHERE CAST(masked_stor_cd AS TEXT) = :store_id
                              AND REPLACE(CAST(prod_dt AS TEXT), '-', '') BETWEEN :date_from AND :date_to
                              {production_item_filter}
                            """
                        ),
                        production_params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return items

        sales_by_item_date_hour: dict[str, dict[str, dict[int, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(float))
        )
        for row in sales_rows:
            item_key = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            if not item_key:
                continue
            sale_dt = str(row.get("sale_dt") or "").strip()
            hour = self._normalize_tmzon_hour(row.get("tmzon_div"))
            if not sale_dt or hour is None:
                continue
            sales_by_item_date_hour[item_key][sale_dt][hour] += self._safe_float(row.get("sale_qty"))

        production_by_item_date: dict[str, dict[str, dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: {"prod1": 0.0, "prod2": 0.0, "sale_prc_sum": 0.0, "sale_prc_count": 0.0})
        )
        for row in production_rows:
            item_key = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            if not item_key:
                continue
            prod_dt = str(row.get("prod_dt") or "").strip()
            if not prod_dt:
                continue
            bucket = production_by_item_date[item_key][prod_dt]
            bucket["prod1"] += self._safe_float(row.get("prod_qty"))
            bucket["prod2"] += self._safe_float(row.get("prod_qty_2"))
            sale_prc = self._safe_float(row.get("sale_prc"))
            if sale_prc > 0:
                bucket["sale_prc_sum"] += sale_prc
                bucket["sale_prc_count"] += 1.0

        enriched_items: list[dict] = []
        for item in items:
            item_key = str(item.get("sku_id") or item.get("name") or "").strip()
            if not item_key:
                enriched_items.append(item)
                continue

            current_stock = self._safe_non_negative_int(item.get("current"))
            daily_sales = sales_by_item_date_hour.get(item_key, {})
            daily_production = production_by_item_date.get(item_key, {})

            avg_first_qty = int(
                round(
                    sum(self._safe_float(daily_production.get(day, {}).get("prod1")) for day in historical_dates)
                    / max(len(historical_dates), 1)
                )
            )
            avg_second_qty = int(
                round(
                    sum(self._safe_float(daily_production.get(day, {}).get("prod2")) for day in historical_dates)
                    / max(len(historical_dates), 1)
                )
            )

            avg_sales_to_reference = sum(
                sum(self._safe_float(daily_sales.get(day, {}).get(hour, 0.0)) for hour in prior_hours)
                for day in historical_dates
            ) / max(len(historical_dates), 1)
            current_sales_to_reference = sum(
                self._safe_float(daily_sales.get(reference_date_text, {}).get(hour, 0.0))
                for hour in prior_hours
            )
            sales_velocity = (
                round(current_sales_to_reference / avg_sales_to_reference, 1)
                if avg_sales_to_reference > 0
                else (1.0 if current_sales_to_reference <= 0 else round(current_sales_to_reference, 1))
            )
            sales_velocity = max(0.5, min(3.0, sales_velocity))

            avg_sales_by_hour: dict[int, float] = {}
            for hour in future_hours:
                avg_sales_by_hour[hour] = sum(
                    self._safe_float(daily_sales.get(day, {}).get(hour, 0.0)) for day in historical_dates
                ) / max(len(historical_dates), 1)

            predicted_sales_1h = int(round(avg_sales_by_hour.get(reference_hour, 0.0) * sales_velocity))
            predicted_sales_1h = max(predicted_sales_1h, 0)
            forecast_stock_1h = max(current_stock - predicted_sales_1h, 0)

            if reference_hour < 14:
                phase_baseline = avg_first_qty
            else:
                phase_baseline = avg_second_qty or avg_first_qty
            historical_baseline = int(
                round((avg_first_qty + avg_second_qty) / max(1, 2 if (avg_first_qty or avg_second_qty) else 1))
            )
            target_stock_level = max(phase_baseline, historical_baseline)
            recommended_qty = max(target_stock_level - forecast_stock_1h, 0)

            remaining_stock = float(current_stock)
            stockout_hour: int | None = None
            chance_loss_qty = 0.0
            for hour in future_hours:
                hour_qty = avg_sales_by_hour.get(hour, 0.0)
                if remaining_stock > 0:
                    if remaining_stock - hour_qty <= 0:
                        stockout_hour = hour
                        chance_loss_qty += max(hour_qty - remaining_stock, 0.0)
                        remaining_stock = 0.0
                    else:
                        remaining_stock -= hour_qty
                else:
                    chance_loss_qty += hour_qty

            predicted_stockout_time = None
            if stockout_hour is not None:
                predicted_stockout_time = f"{max(stockout_hour - reference_hour + 1, 1)}시간 이내"

            sale_prc_total = 0.0
            sale_prc_count = 0.0
            for day in historical_dates:
                sale_prc_total += self._safe_float(daily_production.get(day, {}).get("sale_prc_sum"))
                sale_prc_count += self._safe_float(daily_production.get(day, {}).get("sale_prc_count"))
            unit_price = sale_prc_total / sale_prc_count if sale_prc_count > 0 else 1200.0
            chance_loss_amt = int(round(chance_loss_qty * unit_price))
            chance_loss_reduction_amt = int(round(min(recommended_qty, max(chance_loss_qty, 0.0)) * unit_price))

            enriched = dict(item)
            enriched.update(
                {
                    "forecast": forecast_stock_1h,
                    "predicted_sales_1h": predicted_sales_1h,
                    "recommended": recommended_qty,
                    "prod1": f"08:00 / {avg_first_qty}",
                    "prod2": f"14:00 / {avg_second_qty}",
                    "stockout_expected_at": predicted_stockout_time or str(item.get("stockout_expected_at") or ""),
                    "chance_loss_qty": round(chance_loss_qty, 2),
                    "chance_loss_amt": chance_loss_reduction_amt if chance_loss_reduction_amt > 0 else chance_loss_amt,
                    "chance_loss_reduction_pct": float(
                        chance_loss_reduction_amt if chance_loss_reduction_amt > 0 else chance_loss_amt
                    ),
                    "chance_loss_basis_text": (
                        "추천 생산 수량은 최근 4주 동요일의 1차/2차 생산 평균과 1시간 후 예측 재고를 함께 반영했고, "
                        "찬스 로스 절감액은 동일 요일 평균 판매 패턴 기준 품절 이후 23시까지의 예상 손실 판매를 기준으로 계산했습니다."
                    ),
                    "sales_velocity": sales_velocity,
                }
            )

            if forecast_stock_1h <= 0:
                enriched["status"] = "danger"
            elif forecast_stock_1h <= max(int(round(predicted_sales_1h * 0.5)), 3):
                enriched["status"] = "warning"
            else:
                enriched["status"] = "safe"
            enriched_items.append(enriched)

        return self._finalize_ranked_rows(
            [
                {**row, "_risk_score": max(self._safe_int(row.get("predicted_sales_1h")) - self._safe_int(row.get("current")), 0)}
                for row in enriched_items
            ]
        )

    @staticmethod
    def _resolve_active_item_keys(
        *,
        recent_sales_keys: set[str],
        recent_production_keys: set[str],
        direct_production_keys: set[str],
    ) -> set[str]:
        # 생산 현황 조회 대상:
        # 1) 최근 7일 내 생산 이력이 있는 제품
        # 2) 최근 7일 내 매출 이력이 있으면서 해당 지점 생산 대상인 제품
        direct_keys = set(direct_production_keys)
        return set(recent_production_keys) | (set(recent_sales_keys) & direct_keys)

    @staticmethod
    def _normalize_reference_date(value: str | None) -> str:
        if value:
            return value.replace("-", "")
        return datetime.now().strftime("%Y%m%d")

    @classmethod
    def _resolve_recent_window_bounds(
        cls,
        *,
        reference_date: str | None,
        window_days: int,
    ) -> tuple[str, str]:
        target_dt = datetime.strptime(cls._normalize_reference_date(reference_date), "%Y%m%d").date()
        end_dt = target_dt - timedelta(days=1)
        start_dt = end_dt - timedelta(days=max(window_days - 1, 0))
        return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")

    def _fetch_recent_sales_item_keys(
        self,
        *,
        store_id: str | None,
        reference_date: str | None = None,
        business_date: str | None = None,
        window_days: int = 7,
    ) -> set[str]:
        if not self.engine or not has_table(self.engine, "raw_daily_store_item"):
            return set()
        date_from, date_to = self._resolve_recent_window_bounds(
            reference_date=business_date or reference_date,
            window_days=window_days,
        )
        store_filter_sql = "AND CAST(masked_stor_cd AS TEXT) = :store_id" if store_id else ""
        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT DISTINCT
                                item_cd,
                                item_nm
                            FROM raw_daily_store_item
                            WHERE 1=1
                              {store_filter_sql}
                              AND REPLACE(CAST(sale_dt AS TEXT), '-', '') BETWEEN :date_from AND :date_to
                              AND COALESCE(NULLIF(TRIM(CAST(sale_qty AS TEXT)), '')::numeric, 0) > 0
                            """
                        ),
                        {
                            "store_id": store_id,
                            "date_from": date_from,
                            "date_to": date_to,
                        },
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return set()
        return {
            key
            for row in rows
            for key in self._expand_item_keys(row.get("item_cd"), row.get("item_nm"))
        }

    def _fetch_recent_production_item_keys(
        self,
        *,
        store_id: str | None,
        reference_date: str | None = None,
        business_date: str | None = None,
        window_days: int = 7,
    ) -> set[str]:
        if not self.engine or not has_table(self.engine, "raw_production_extract"):
            return set()
        date_from, date_to = self._resolve_recent_window_bounds(
            reference_date=business_date or reference_date,
            window_days=window_days,
        )
        store_filter_sql = "AND CAST(masked_stor_cd AS TEXT) = :store_id" if store_id else ""
        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT DISTINCT
                                item_cd,
                                item_nm
                            FROM raw_production_extract
                            WHERE 1=1
                              {store_filter_sql}
                              AND REPLACE(CAST(prod_dt AS TEXT), '-', '') BETWEEN :date_from AND :date_to
                              AND (
                                COALESCE(NULLIF(TRIM(CAST(prod_qty AS TEXT)), '')::numeric, 0) > 0
                                OR COALESCE(NULLIF(TRIM(CAST(prod_qty_2 AS TEXT)), '')::numeric, 0) > 0
                                OR COALESCE(NULLIF(TRIM(CAST(prod_qty_3 AS TEXT)), '')::numeric, 0) > 0
                                OR COALESCE(NULLIF(TRIM(CAST(reprod_qty AS TEXT)), '')::numeric, 0) > 0
                              )
                            """
                        ),
                        {
                            "store_id": store_id,
                            "date_from": date_from,
                            "date_to": date_to,
                        },
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return set()
        return {
            key
            for row in rows
            for key in self._expand_item_keys(row.get("item_cd"), row.get("item_nm"))
        }

    def _fetch_direct_production_item_keys(self, *, store_id: str | None) -> set[str]:
        if not self.engine or not store_id:
            return set()
        if has_table(self.engine, "raw_store_production_item"):
            try:
                with self.engine.connect() as connection:
                    rows = (
                        connection.execute(
                            text(
                                """
                                SELECT DISTINCT
                                    item_cd,
                                    item_nm
                                FROM raw_store_production_item
                                WHERE CAST(masked_stor_cd AS TEXT) = :store_id
                                """
                            ),
                            {"store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
            except SQLAlchemyError:
                rows = []
            direct_keys = {
                key
                for row in rows
                for key in self._expand_item_keys(row.get("item_cd"), row.get("item_nm"))
            }
            if direct_keys:
                return direct_keys

        if not has_table(self.engine, "raw_production_extract"):
            return set()
        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            """
                            SELECT DISTINCT
                                item_cd,
                                item_nm
                            FROM raw_production_extract
                            WHERE CAST(masked_stor_cd AS TEXT) = :store_id
                              AND (
                                COALESCE(NULLIF(TRIM(CAST(prod_qty AS TEXT)), '')::numeric, 0) > 0
                                OR COALESCE(NULLIF(TRIM(CAST(prod_qty_2 AS TEXT)), '')::numeric, 0) > 0
                                OR COALESCE(NULLIF(TRIM(CAST(prod_qty_3 AS TEXT)), '')::numeric, 0) > 0
                                OR COALESCE(NULLIF(TRIM(CAST(reprod_qty AS TEXT)), '')::numeric, 0) > 0
                              )
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return set()
        return {
            key
            for row in rows
            for key in self._expand_item_keys(row.get("item_cd"), row.get("item_nm"))
        }

    def _fetch_store_production_item_keys(self, store_id: str | None) -> set[str]:
        return self._fetch_direct_production_item_keys(store_id=store_id)

    def _list_items_from_mart_production_status(
        self,
        store_id: str | None,
        business_date: str | None,
        reference_datetime: datetime | None = None,
    ) -> list[dict]:
        if not self.engine or not hasattr(self.engine, "connect"):
            return []

        table_name = self._get_production_inventory_mart_table(store_id)
        if not table_name or not store_id:
            return []

        normalized_business_date = (
            str(business_date).replace("-", "").strip() if business_date else None
        )
        recent_sales_keys = self._fetch_recent_sales_item_keys(
            store_id=store_id,
            business_date=business_date,
        )
        recent_production_keys = self._fetch_recent_production_item_keys(
            store_id=store_id,
            business_date=business_date,
        )
        direct_production_keys = self._fetch_store_production_item_keys(store_id)
        active_keys = self._resolve_active_item_keys(
            recent_sales_keys=recent_sales_keys,
            recent_production_keys=recent_production_keys,
            direct_production_keys=direct_production_keys,
        )

        try:
            with self.engine.connect() as connection:
                if normalized_business_date:
                    snapshot_row = (
                        connection.execute(
                            text(
                                f"""
                                SELECT business_date
                                FROM {table_name}
                                WHERE store_id = :store_id
                                  AND business_date <= :business_date
                                ORDER BY business_date DESC
                                LIMIT 1
                                """
                            ),
                            {"store_id": store_id, "business_date": normalized_business_date},
                        )
                        .mappings()
                        .first()
                    )
                else:
                    snapshot_row = (
                        connection.execute(
                            text(
                                f"""
                                SELECT business_date
                                FROM {table_name}
                                WHERE store_id = :store_id
                                ORDER BY business_date DESC
                                LIMIT 1
                                """
                            ),
                            {"store_id": store_id},
                        )
                        .mappings()
                        .first()
                    )

                if not snapshot_row:
                    return []

                rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT
                                item_cd,
                                item_nm,
                                total_stock,
                                stock_rate,
                                status,
                                stockout_hour
                            FROM {table_name}
                            WHERE store_id = :store_id
                              AND business_date = :business_date
                            ORDER BY
                                CASE status
                                    WHEN 'shortage' THEN 0
                                    WHEN 'warning' THEN 1
                                    ELSE 2
                                END,
                                total_stock ASC,
                                item_nm ASC
                            """
                        ),
                        {
                            "store_id": store_id,
                            "business_date": str(snapshot_row.get("business_date") or ""),
                        },
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError as exc:
            logger.warning(
                "_list_items_from_mart_production_status query failed: store_id=%s business_date=%s error=%s",
                store_id,
                business_date,
                exc,
            )
            return []

        items: list[dict] = []
        for row in rows:
            sku_id = str(row.get("item_cd") or "").strip()
            name = str(row.get("item_nm") or "").strip()
            if active_keys and sku_id not in active_keys and name not in active_keys:
                continue

            current_stock = self._safe_non_negative_int(row.get("total_stock"))
            status = str(row.get("status") or "normal").strip().lower()
            normalized_status = "safe"
            if status == "shortage":
                normalized_status = "danger"
            elif status == "warning":
                normalized_status = "warning"

            stockout_hour = row.get("stockout_hour")
            first_time = "08:00"
            first_qty = 0
            second_time = "14:00"
            second_qty = 0
            items.append(
                {
                    "sku_id": sku_id or name,
                    "name": name or sku_id,
                    "current": current_stock,
                    "forecast": current_stock,
                    "predicted_sales_1h": 0,
                    "order_confirm_qty": 0,
                    "hourly_sale_qty": 0,
                    "status": normalized_status,
                    "depletion_time": "-",
                    "recommended": 0,
                    "prod1": f"{first_time} / {first_qty}개",
                    "prod2": f"{second_time} / {second_qty}개",
                    "stockout_expected_at": str(row.get("predicted_stockout_time") or ""),
                    "chance_loss_reduction_pct": self._safe_float(
                        row.get("chance_loss_saving_pct")
                    ),
                    "chance_loss_amt": self._safe_float(row.get("chance_loss_saving_pct")),
                    "chance_loss_basis_text": str(row.get("chance_loss_basis_text") or ""),
                    "alert_message": str(row.get("alert_message") or ""),
                    "sales_velocity": self._safe_float(row.get("sales_velocity")),
                    "speed_alert": bool(row.get("speed_alert") or False),
                    "speed_alert_message": str(row.get("speed_alert_message") or ""),
                    "material_alert": bool(row.get("material_alert") or False),
                    "material_alert_message": str(row.get("material_alert_message") or ""),
                    "can_produce": bool(row.get("can_produce") if row.get("can_produce") is not None else True),
                }
            )
            items[-1].update(
                {
                    "prod1": "08:00 / 0개",
                    "prod2": "14:00 / 0개",
                    "stockout_expected_at": (
                        str(stockout_hour).strip() if stockout_hour not in (None, "") else ""
                    ),
                    "chance_loss_reduction_pct": None,
                    "chance_loss_amt": None,
                    "chance_loss_basis_text": "",
                    "alert_message": "",
                    "sales_velocity": self._safe_float(row.get("stock_rate")),
                    "speed_alert": False,
                    "speed_alert_message": "",
                    "material_alert": False,
                    "material_alert_message": "",
                    "can_produce": True,
                }
            )
        return self._enrich_items_with_historical_metrics(
            items=items,
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )

    def _list_items_from_prediction_snapshot(
        self,
        store_id: str | None,
        business_date: str | None,
    ) -> list[dict]:
        if (
            not self.engine
            or not has_table(self.engine, "production_prediction_snapshots")
            or not has_table(self.engine, "production_prediction_snapshot_items")
        ):
            return []
        if not hasattr(self.engine, "connect"):
            return []
        normalized_business_date = (
            str(business_date).replace("-", "").strip() if business_date else None
        )
        try:
            with self.engine.connect() as connection:
                snapshot = None
                if normalized_business_date:
                    snapshot = (
                        connection.execute(
                            text(
                                """
                                SELECT id, target_hour, business_date
                                FROM production_prediction_snapshots
                                WHERE masked_stor_cd = :store_id
                                  AND business_date = :business_date
                                  AND status = 'completed'
                                ORDER BY created_at DESC
                                LIMIT 1
                                """
                            ),
                            {
                                "store_id": store_id,
                                "business_date": normalized_business_date,
                            },
                        )
                        .mappings()
                        .first()
                    )
                if not snapshot:
                    snapshot = (
                        connection.execute(
                            text(
                                """
                                SELECT id, target_hour, business_date
                                FROM production_prediction_snapshots
                                WHERE masked_stor_cd = :store_id
                                  AND status = 'completed'
                                ORDER BY business_date DESC, target_hour DESC, created_at DESC
                                LIMIT 1
                                """
                            ),
                            {"store_id": store_id},
                        )
                        .mappings()
                        .first()
                    )
                if not snapshot:
                    return []
                rows = (
                    connection.execute(
                        text(
                            """
                            SELECT
                                sku_id,
                                name,
                                current_stock,
                                predicted_stock_1h,
                                forecast_baseline,
                                recommended_production_qty,
                                avg_first_production_qty_4w,
                                avg_first_production_time_4w,
                                avg_second_production_qty_4w,
                                avg_second_production_time_4w,
                                order_confirm_qty,
                                hourly_sale_qty,
                                status,
                                depletion_time,
                                stockout_expected_at,
                                alert_message,
                                confidence,
                                chance_loss_qty,
                                chance_loss_amt,
                                chance_loss_reduction_pct
                            FROM production_prediction_snapshot_items
                            WHERE snapshot_id = :snapshot_id
                            ORDER BY sku_id
                            """
                        ),
                        {"snapshot_id": int(snapshot.get("id") or 0)},
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return []

        items: list[dict] = []
        for row in rows:
            first_qty = self._safe_non_negative_int(row.get("avg_first_production_qty_4w"))
            second_qty = self._safe_non_negative_int(row.get("avg_second_production_qty_4w"))
            first_time = str(row.get("avg_first_production_time_4w") or "08:00")
            second_time = str(row.get("avg_second_production_time_4w") or "14:00")
            current_stock = self._safe_non_negative_int(row.get("current_stock"))
            forecast_stock_1h = min(
                current_stock,
                max(self._safe_int(row.get("predicted_stock_1h")), 0),
            )
            items.append(
                {
                    "sku_id": str(row.get("sku_id") or ""),
                    "name": str(row.get("name") or ""),
                    "current": current_stock,
                    "forecast": forecast_stock_1h,
                    "predicted_sales_1h": max(current_stock - forecast_stock_1h, 0),
                    "order_confirm_qty": self._safe_non_negative_int(row.get("order_confirm_qty")),
                    "hourly_sale_qty": self._safe_non_negative_int(row.get("hourly_sale_qty")),
                    "status": str(row.get("status") or "normal"),
                    "depletion_time": str(row.get("depletion_time") or "-"),
                    "recommended": self._safe_non_negative_int(row.get("recommended_production_qty")),
                    "prod1": f"{first_time} / {first_qty}개",
                    "prod2": f"{second_time} / {second_qty}개",
                    "stockout_expected_at": str(row.get("stockout_expected_at") or ""),
                    "alert_message": str(row.get("alert_message") or ""),
                    "confidence": self._safe_float(row.get("confidence")),
                    "chance_loss_qty": self._safe_float(row.get("chance_loss_qty")),
                    "chance_loss_amt": self._safe_float(row.get("chance_loss_amt")),
                    "chance_loss_reduction_pct": self._safe_float(row.get("chance_loss_reduction_pct")),
                    "snapshot_business_date": str(snapshot.get("business_date") or ""),
                    "snapshot_target_hour": self._safe_int(snapshot.get("target_hour")),
                }
            )
        return items

    async def list_items(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> list[dict]:
        mart_items = self._list_items_from_mart_production_status(
            store_id=store_id,
            business_date=business_date,
        )
        if mart_items:
            return self._enrich_items_with_historical_metrics(
                items=mart_items,
                store_id=store_id,
                business_date=business_date,
                reference_datetime=reference_datetime,
            )

        snapshot_items = self._list_items_from_prediction_snapshot(
            store_id=store_id,
            business_date=business_date,
        )
        if snapshot_items:
            return self._enrich_items_with_historical_metrics(
                items=snapshot_items,
                store_id=store_id,
                business_date=business_date,
                reference_datetime=reference_datetime,
            )

        production_map: dict[str, dict[str, object]] = {}
        secondary_map: dict[str, dict[str, object]] = {}
        stock_map: dict[str, dict[str, object]] = {}
        sale_map: dict[str, dict[str, object]] = {}
        order_confirm_map: dict[str, dict[str, object]] = {}
        hourly_sale_map: dict[str, dict[str, object]] = {}

        production_reference_date = business_date
        if business_date:
            try:
                production_reference_date = (
                    datetime.strptime(business_date, "%Y-%m-%d") - timedelta(days=1)
                ).strftime("%Y%m%d")
            except ValueError:
                production_reference_date = business_date

        if self.engine and has_table(self.engine, "raw_production_extract"):
            # 4주(28일) 발생일 기준 일평균 수량 집계
            production_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty",),
                store_id=store_id,
                window_days=28,
                reference_date=production_reference_date,
            )
            secondary_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty_2", "reprod_qty", "prod_qty_3"),
                store_id=store_id,
                window_days=28,
                reference_date=production_reference_date,
            )
        else:
            logger.warning(
                "list_items: raw_production_extract 테이블 없음 (engine=%s)", bool(self.engine)
            )

        if self.engine and has_table(self.engine, "raw_inventory_extract"):
            stock_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("stock_qty",),
                store_id=store_id,
                reference_date=business_date,
            )
        else:
            logger.warning(
                "list_items: raw_inventory_extract 테이블 없음 (engine=%s)", bool(self.engine)
            )

        if self.engine and has_table(self.engine, "raw_daily_store_item"):
            sale_map = self._fetch_metric_map(
                "raw_daily_store_item",
                ("sale_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
                reference_date=business_date,
            )
        elif self.engine and has_table(self.engine, "raw_inventory_extract"):
            sale_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
                reference_date=business_date,
            )

        if self.engine and has_table(self.engine, "raw_order_extract") and not business_date:
            order_confirm_map = self._fetch_metric_map(
                "raw_order_extract",
                ("dlv_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("confrm_qty",),
                store_id=store_id,
                window_days=14,
                reference_date=business_date,
            )

        if self.engine and has_table(self.engine, "core_hourly_item_sales"):
            hourly_sale_map = self._fetch_metric_map(
                "core_hourly_item_sales",
                ("sale_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
                window_days=7,
                reference_date=business_date,
            )

        recent_production_keys = self._fetch_recent_production_item_keys(
            store_id=store_id,
            business_date=business_date,
        )
        recent_sales_keys = self._fetch_recent_sales_item_keys(
            store_id=store_id,
            business_date=business_date,
        )
        direct_production_keys = self._fetch_store_production_item_keys(store_id)

        active_keys = self._resolve_active_item_keys(
            recent_sales_keys=recent_sales_keys,
            recent_production_keys=recent_production_keys,
            direct_production_keys=direct_production_keys,
        )

        items = self._build_new_items(
            production_map,
            secondary_map,
            stock_map,
            sale_map,
            order_confirm_map,
            hourly_sale_map,
            active_keys=active_keys,
        )
        if items:
            logger.debug(
                "list_items: raw 테이블 기준 %d건 반환 (store_id=%s)", len(items), store_id
            )
        else:
            logger.warning("list_items: 데이터 없음 (store_id=%s)", store_id)
        return items

    async def fetch_simulation_data(
        self,
        store_id: str,
        item_id: str,
        simulation_date: str,
        window_days: int = 30,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """시뮬레이션용 raw 데이터를 조회합니다. (inventory_data, production_data, sales_data) 반환."""
        if not self.engine:
            return [], [], []

        inventory_data: list[dict] = []
        production_data: list[dict] = []
        sales_data: list[dict] = []

        try:
            target_dt = datetime.strptime(simulation_date, "%Y-%m-%d")
            date_from = (target_dt - timedelta(days=window_days)).strftime("%Y%m%d")

            with self.engine.connect() as conn:
                if has_table(self.engine, "raw_inventory_extract"):
                    rows = (
                        conn.execute(
                            text(
                                """
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(stock_qty::NUMERIC, 0)           AS "STOCK_QTY",
                                COALESCE(sale_qty::NUMERIC, 0)            AS "SALE_QTY",
                                CAST(stock_dt AS TEXT)                    AS "STOCK_DT"
                            FROM raw_inventory_extract
                            WHERE CAST(stock_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                            ),
                            {"date_from": date_from, "store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
                    inventory_data = [dict(r) for r in rows]

                if has_table(self.engine, "raw_production_extract"):
                    production_columns = self._table_columns("raw_production_extract")
                    prod_degree_col = self._pick_column(
                        production_columns, ("prod_dgre", "prod_degree")
                    )
                    sale_price_col = self._pick_column(
                        production_columns, ("sale_prc", "sale_price")
                    )
                    item_cost_col = self._pick_column(
                        production_columns, ("item_cost", "cost_amt")
                    )
                    prod_degree_expr = (
                        f"COALESCE(CAST({prod_degree_col} AS TEXT), '')"
                        if prod_degree_col
                        else "''"
                    )
                    sale_price_expr = (
                        f"COALESCE(CAST({sale_price_col} AS NUMERIC), 0)"
                        if sale_price_col
                        else "0"
                    )
                    item_cost_expr = (
                        f"COALESCE(CAST({item_cost_col} AS NUMERIC), 0)"
                        if item_cost_col
                        else "0"
                    )
                    rows = (
                        conn.execute(
                            text(
                                f"""
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(prod_qty::NUMERIC, 0)            AS "PROD_QTY",
                                CAST(prod_dt AS TEXT)                     AS "PROD_DT",
                                {prod_degree_expr}                        AS "PROD_DGRE",
                                {sale_price_expr}                         AS "SALE_PRC",
                                {item_cost_expr}                          AS "ITEM_COST"
                            FROM raw_production_extract
                            WHERE CAST(prod_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                            ),
                            {"date_from": date_from, "store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
                    production_data = [dict(r) for r in rows]

                if has_table(self.engine, "raw_daily_store_item_tmzon"):
                    rows = (
                        conn.execute(
                            text(
                                """
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(sale_qty::NUMERIC, 0)            AS "SALE_QTY",
                                CAST(sale_dt AS TEXT)                     AS "SALE_DT",
                                CAST(tmzon_div AS TEXT)                   AS "TMZON_DIV"
                            FROM raw_daily_store_item_tmzon
                            WHERE CAST(sale_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                            ),
                            {"date_from": date_from, "store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
                    sales_data = [dict(r) for r in rows]
        except (SQLAlchemyError, ValueError):
            return [], [], []

        return inventory_data, production_data, sales_data

    async def save_registration(self, payload: dict) -> dict:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.begin() as connection:
                    connection.execute(
                        text(
                            """
                            INSERT INTO production_registrations(
                                sku_id, qty, registered_by, feedback_type, feedback_message, store_id
                            ) VALUES (
                                :sku_id, :qty, :registered_by, :feedback_type, :feedback_message, :store_id
                            )
                            """
                        ),
                        {
                            **payload,
                            "feedback_type": "chance_loss_reduced",
                            "feedback_message": "재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
                        },
                    )
                    return payload
            except SQLAlchemyError:
                return {**payload, "saved": False}
        return {**payload, "saved": False}

    async def list_registration_history(
        self,
        limit: int = 20,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(
                        store_id=store_id, date_from=date_from, date_to=date_to
                    )
                    rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                sku_id,
                                qty,
                                registered_by,
                                COALESCE(feedback_type, 'chance_loss_reduced') AS feedback_type,
                                COALESCE(feedback_message, '재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.') AS feedback_message,
                                TO_CHAR(registered_at, 'YYYY-MM-DD HH24:MI:SS') AS registered_at,
                                store_id
                            FROM production_registrations
                            {filter_clause}
                            ORDER BY registered_at DESC
                            LIMIT :limit
                            """
                            ),
                            {"limit": limit, **params},
                        )
                        .mappings()
                        .all()
                    )
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                return []
        return []

    def get_stock_rate_recent_rows(self, store_id: str) -> list[dict]:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH ranked AS (
                                SELECT
                                    masked_stor_cd,
                                    prc_dt,
                                    item_cd,
                                    item_nm,
                                    COALESCE(ord_avg, 0) AS ord_avg,
                                    COALESCE(sal_avg, 0) AS sal_avg,
                                    COALESCE(stk_avg, 0) AS stk_avg,
                                    COALESCE(stk_rt, 0) AS stk_rt,
                                    DENSE_RANK() OVER (ORDER BY prc_dt DESC) AS dr
                                FROM core_stock_rate
                                WHERE masked_stor_cd = :store_id
                            )
                            SELECT
                                masked_stor_cd,
                                prc_dt,
                                item_cd,
                                item_nm,
                                ord_avg,
                                sal_avg,
                                stk_avg,
                                stk_rt,
                                dr
                            FROM ranked
                            WHERE dr <= 2
                            ORDER BY item_nm, dr
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .all()
                )
            mapped_rows = [dict(r) for r in rows]
            if mapped_rows:
                return mapped_rows
            return self._get_stock_rate_recent_rows_fallback(store_id=store_id)
        except SQLAlchemyError as exc:
            logger.warning("get_stock_rate_recent_rows 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return self._get_stock_rate_recent_rows_fallback(store_id=store_id)

    def get_stockout_latest_rows(self, store_id: str) -> list[dict]:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH latest_date AS (
                                SELECT MAX(prc_dt) AS prc_dt
                                FROM core_stockout_time
                                WHERE masked_stor_cd = :store_id
                            )
                            SELECT
                                item_cd,
                                item_nm,
                                is_stockout,
                                stockout_hour
                            FROM core_stockout_time s
                            JOIN latest_date d ON s.prc_dt = d.prc_dt
                            WHERE s.masked_stor_cd = :store_id
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .all()
                )
            mapped_rows = [dict(r) for r in rows]
            if mapped_rows:
                return mapped_rows
            return self._get_stockout_latest_rows_fallback(store_id=store_id)
        except SQLAlchemyError as exc:
            logger.warning("get_stockout_latest_rows 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return self._get_stockout_latest_rows_fallback(store_id=store_id)

    def get_production_waste_rows(
        self,
        store_id: str,
        date_from: str,
        date_to: str,
    ) -> list[dict]:
        """???? ?? ?? ??? ??.

        ?? lot? ???? ??, ????? ?? FIFO ???? ??? ?
        ????? ?? ??? ?? ??? ????? ????.
        date_from / date_to: YYYYMMDD ??
        """
        if not self.engine:
            return []
        waste_mart_table = self._production_waste_daily_mart_table(store_id)
        if waste_mart_table:
            try:
                with self.engine.connect() as conn:
                    rows = (
                        conn.execute(
                            text(
                                f"""
                                SELECT
                                    target_date,
                                    item_cd,
                                    item_nm,
                                    total_waste_qty,
                                    total_waste_amount,
                                    adjusted_loss_qty,
                                    adjusted_loss_amount,
                                    estimated_expiry_loss_qty,
                                    assumed_shelf_life_days,
                                    expiry_risk_level
                                FROM {waste_mart_table}
                                WHERE store_id = :store_id
                                  AND target_date BETWEEN :date_from AND :date_to
                                ORDER BY target_date DESC, total_waste_amount DESC, total_waste_qty DESC, item_nm
                                """
                            ),
                            {
                                "store_id": store_id,
                                "date_from": date_from,
                                "date_to": date_to,
                            },
                        )
                        .mappings()
                        .all()
                    )
            except SQLAlchemyError:
                return []
            return [dict(row) for row in rows]
        if self._production_waste_daily_mart_configured(store_id):
            return []
        try:
            target_start = self._parse_yyyymmdd(date_from)
            lookback_start = (target_start - timedelta(days=60)).strftime("%Y%m%d")
            with self.engine.connect() as conn:
                production_rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                p.prod_dt,
                                COALESCE(NULLIF(TRIM(p.item_cd), ''), NULLIF(TRIM(p.item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(p.item_nm), ''), NULLIF(TRIM(p.item_cd), '')) AS item_nm,
                                SUM(
                                    COALESCE(NULLIF(TRIM(p.prod_qty), '')::numeric, 0)
                                    + COALESCE(NULLIF(TRIM(p.prod_qty_2), '')::numeric, 0)
                                    + COALESCE(NULLIF(TRIM(p.prod_qty_3), '')::numeric, 0)
                                    + COALESCE(NULLIF(TRIM(p.reprod_qty), '')::numeric, 0)
                                ) AS produced_qty
                            FROM raw_production_extract p
                            WHERE p.masked_stor_cd = :store_id
                              AND p.prod_dt BETWEEN :lookback_start AND :date_to
                            GROUP BY
                                p.prod_dt,
                                COALESCE(NULLIF(TRIM(p.item_cd), ''), NULLIF(TRIM(p.item_nm), '')),
                                COALESCE(NULLIF(TRIM(p.item_nm), ''), NULLIF(TRIM(p.item_cd), ''))
                            HAVING SUM(
                                COALESCE(NULLIF(TRIM(p.prod_qty), '')::numeric, 0)
                                + COALESCE(NULLIF(TRIM(p.prod_qty_2), '')::numeric, 0)
                                + COALESCE(NULLIF(TRIM(p.prod_qty_3), '')::numeric, 0)
                                + COALESCE(NULLIF(TRIM(p.reprod_qty), '')::numeric, 0)
                            ) > 0
                            """
                        ),
                        {"store_id": store_id, "lookback_start": lookback_start, "date_to": date_to},
                    )
                    .mappings()
                    .all()
                )
                sales_rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                s.sale_dt,
                                COALESCE(NULLIF(TRIM(s.item_cd), ''), NULLIF(TRIM(s.item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(s.item_nm), ''), NULLIF(TRIM(s.item_cd), '')) AS item_nm,
                                SUM(COALESCE(s.sale_qty, 0)) AS sale_qty
                            FROM core_daily_item_sales s
                            WHERE s.masked_stor_cd = :store_id
                              AND s.sale_dt BETWEEN :lookback_start AND :date_to
                            GROUP BY
                                s.sale_dt,
                                COALESCE(NULLIF(TRIM(s.item_cd), ''), NULLIF(TRIM(s.item_nm), '')),
                                COALESCE(NULLIF(TRIM(s.item_nm), ''), NULLIF(TRIM(s.item_cd), ''))
                            HAVING SUM(COALESCE(s.sale_qty, 0)) > 0
                            """
                        ),
                        {"store_id": store_id, "lookback_start": lookback_start, "date_to": date_to},
                    )
                    .mappings()
                    .all()
                )
                unit_price_rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')) AS item_cd,
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), '')) AS item_nm,
                                AVG(actual_sale_amt / NULLIF(sale_qty, 0)) AS avg_unit_price
                            FROM core_daily_item_sales
                            WHERE masked_stor_cd = :store_id
                              AND sale_dt BETWEEN :lookback_start AND :date_to
                              AND sale_qty > 0
                            GROUP BY
                                COALESCE(NULLIF(TRIM(item_cd), ''), NULLIF(TRIM(item_nm), '')),
                                COALESCE(NULLIF(TRIM(item_nm), ''), NULLIF(TRIM(item_cd), ''))
                            """
                        ),
                        {"store_id": store_id, "lookback_start": lookback_start, "date_to": date_to},
                    )
                    .mappings()
                    .all()
                )

            shelf_life_map = self.get_shelf_life_days_map(
                item_codes=[str(row.get("item_cd") or "").strip() for row in production_rows],
                item_names=[str(row.get("item_nm") or "").strip() for row in production_rows],
            )
            unit_price_map: dict[str, float] = {}
            for row in unit_price_rows:
                item_cd = str(row.get("item_cd") or "").strip()
                item_nm = str(row.get("item_nm") or "").strip()
                normalized_name_key = self._normalize_menu_name_key(item_nm)
                avg_unit_price = self._safe_float(row.get("avg_unit_price"))
                for key in (item_cd, item_nm, normalized_name_key):
                    if not key:
                        continue
                    current_price = self._safe_float(unit_price_map.get(key))
                    if key not in unit_price_map or (current_price <= 0 and avg_unit_price > 0):
                        unit_price_map[key] = avg_unit_price

            return self._compute_expiry_waste_rows(
                production_rows=[dict(r) for r in production_rows],
                sales_rows=[dict(r) for r in sales_rows],
                unit_price_map=unit_price_map,
                shelf_life_map=shelf_life_map,
                date_from=date_from,
                date_to=date_to,
            )
        except SQLAlchemyError as exc:
            logger.warning(
                "get_production_waste_rows ?? ??: store_id=%s error=%s",
                store_id,
                exc,
            )
            return []

    def get_inventory_status(
        self,
        store_id: str | None = None,
        page: int = 1,
        page_size: int = 10,
        status_filters: list[str] | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> tuple[list[dict], int, dict]:
        if not self.engine or not store_id:
            return [], 0, {}
        try:
            offset = max(0, (page - 1) * page_size)
            effective_business_date = business_date
            if effective_business_date is None and reference_datetime is not None:
                effective_business_date = reference_datetime.strftime("%Y-%m-%d")
            normalized_business_date = (
                effective_business_date.replace("-", "") if effective_business_date else None
            )
            status_filter_clause, status_filter_params = self._build_inventory_status_filter_clause(
                status_filters=status_filters
            )
            mart_status_filter_clause = self._build_inventory_mart_filter_clause(
                status_filters=status_filters,
                stock_rate_column="stock_rate",
                is_stockout_column="is_stockout",
            )
            with self.engine.connect() as conn:
                inventory_mart_table = self._get_production_inventory_mart_table(store_id)
                if inventory_mart_table:
                    if normalized_business_date:
                        latest_business_date = conn.execute(
                            text(
                                f"""
                                SELECT MAX(business_date)
                                FROM {inventory_mart_table}
                                WHERE store_id = :store_id
                                  AND business_date <= :business_date
                                """
                            ),
                            {"store_id": store_id, "business_date": normalized_business_date},
                        ).scalar_one_or_none()
                    else:
                        latest_business_date = conn.execute(
                            text(
                                f"""
                                SELECT MAX(business_date)
                                FROM {inventory_mart_table}
                                WHERE store_id = :store_id
                                """
                            ),
                            {"store_id": store_id},
                        ).scalar_one_or_none()
                    if latest_business_date:
                        total_items = int(
                            conn.execute(
                                text(
                                    f"""
                                    SELECT COUNT(*)
                                    FROM {inventory_mart_table}
                                    WHERE store_id = :store_id
                                      AND business_date = :business_date
                                      {mart_status_filter_clause}
                                    """
                                ),
                                {
                                    "store_id": store_id,
                                    "business_date": latest_business_date,
                                    **status_filter_params,
                                },
                            ).scalar_one()
                        )
                        summary_row = conn.execute(
                            text(
                                f"""
                                SELECT
                                    COUNT(*) FILTER (WHERE COALESCE(is_stockout, FALSE) = TRUE OR COALESCE(stock_rate, 0) < 0) AS shortage_count,
                                    COUNT(*) FILTER (WHERE COALESCE(is_stockout, FALSE) = FALSE AND COALESCE(stock_rate, 0) >= 0.35) AS excess_count,
                                    COUNT(*) FILTER (WHERE COALESCE(is_stockout, FALSE) = FALSE AND COALESCE(stock_rate, 0) >= 0 AND COALESCE(stock_rate, 0) < 0.35) AS normal_count,
                                    AVG(stock_rate) AS avg_stock_rate
                                FROM {inventory_mart_table}
                                WHERE store_id = :store_id
                                  AND business_date = :business_date
                                  {mart_status_filter_clause}
                                """
                            ),
                            {
                                "store_id": store_id,
                                "business_date": latest_business_date,
                                **status_filter_params,
                            },
                        ).mappings().one()
                        rows = (
                            conn.execute(
                                text(
                                    f"""
                                    SELECT
                                        m.item_cd,
                                        m.item_nm,
                                        COALESCE(cat.category, psl.item_group) AS item_group,
                                        m.total_stock AS stk_avg,
                                        m.total_sold AS sal_avg,
                                        m.total_orderable AS ord_avg,
                                        m.stock_rate AS stk_rt,
                                        m.is_stockout,
                                        m.stockout_hour,
                                        m.assumed_shelf_life_days,
                                        m.expiry_risk_level,
                                        CASE
                                            WHEN COALESCE(m.is_stockout, FALSE) = TRUE OR COALESCE(m.stock_rate, 0) < 0 THEN '부족'
                                            WHEN COALESCE(m.is_stockout, FALSE) = FALSE AND COALESCE(m.stock_rate, 0) >= 0.35 THEN '여유'
                                            ELSE '적정'
                                        END AS status
                                    FROM {inventory_mart_table} AS m
                                    LEFT JOIN LATERAL (
                                        SELECT MAX(COALESCE(NULLIF(TRIM(CAST(mic.category AS TEXT)), ''), '기타')) AS category
                                        FROM mart_item_category_master mic
                                        WHERE (
                                            COALESCE(NULLIF(TRIM(CAST(mic.item_cd AS TEXT)), ''), '') <> ''
                                            AND COALESCE(NULLIF(TRIM(CAST(mic.item_cd AS TEXT)), ''), '') =
                                                COALESCE(NULLIF(TRIM(CAST(m.item_cd AS TEXT)), ''), '')
                                        )
                                        OR COALESCE(NULLIF(TRIM(CAST(mic.item_nm AS TEXT)), ''), '') =
                                           COALESCE(NULLIF(TRIM(CAST(m.item_nm AS TEXT)), ''), '')
                                        OR (
                                            COALESCE(NULLIF(TRIM(CAST(mic.parent_item_nm AS TEXT)), ''), '') <> ''
                                            AND COALESCE(NULLIF(TRIM(CAST(mic.parent_item_nm AS TEXT)), ''), '') =
                                                COALESCE(NULLIF(TRIM(CAST(m.item_nm AS TEXT)), ''), '')
                                        )
                                    ) cat ON TRUE
                                    LEFT JOIN raw_product_shelf_life AS psl
                                        ON psl.item_cd = m.item_cd
                                    WHERE m.store_id = :store_id
                                      AND m.business_date = :business_date
                                      {mart_status_filter_clause}
                                    ORDER BY m.stock_rate ASC, m.item_nm ASC
                                    LIMIT :page_size OFFSET :offset
                                    """
                                ),
                                {
                                    "store_id": store_id,
                                    "business_date": latest_business_date,
                                    "page_size": page_size,
                                    "offset": offset,
                                    **status_filter_params,
                                },
                            )
                            .mappings()
                            .all()
                        )
                        return [dict(r) for r in rows], total_items, dict(summary_row)
                    if self._production_inventory_mart_configured(store_id):
                        return [], 0, {}
                elif self._production_inventory_mart_configured(store_id):
                    return [], 0, {}
                base_cte_sql = f"""
                    WITH latest AS (
                        SELECT DISTINCT ON (sr.item_cd)
                            sr.item_cd,
                            sr.item_nm,
                            sr.stk_avg,
                            sr.sal_avg,
                            sr.ord_avg,
                            sr.stk_rt,
                            COALESCE(st.is_stockout, FALSE) AS is_stockout,
                            st.stockout_hour,
                            CASE
                                WHEN sr.stk_rt < 0 OR COALESCE(st.is_stockout, FALSE) THEN '부족'
                                WHEN sr.stk_rt >= 0.35 AND NOT COALESCE(st.is_stockout, FALSE) THEN '여유'
                                ELSE '적정'
                            END AS status
                        FROM core_stock_rate sr
                        LEFT JOIN core_stockout_time st
                            ON sr.masked_stor_cd = st.masked_stor_cd
                           AND sr.item_cd = st.item_cd
                           AND sr.prc_dt = st.prc_dt
                        WHERE sr.masked_stor_cd = :store_id
                          AND (CAST(:business_date AS TEXT) IS NULL OR sr.prc_dt <= :business_date)
                        ORDER BY sr.item_cd, sr.prc_dt DESC
                    ),
                    filtered AS (
                        SELECT *
                        FROM latest
                        WHERE 1 = 1
                        {status_filter_clause}
                    )
                """
                base_params = {
                    "store_id": store_id,
                    "business_date": normalized_business_date,
                    **status_filter_params,
                }
                summary_row = conn.execute(
                    text(
                        f"""
                        {base_cte_sql}
                        SELECT
                            COUNT(*) FILTER (WHERE status = '부족') AS shortage_count,
                            COUNT(*) FILTER (WHERE status = '여유') AS excess_count,
                            COUNT(*) FILTER (WHERE status = '적정') AS normal_count,
                            AVG(stk_rt) AS avg_stock_rate
                        FROM filtered
                        """
                    ),
                    base_params,
                ).mappings().one()
                summary_metrics = dict(summary_row)
                total_items = int(
                    conn.execute(
                        text(
                            f"""
                            {base_cte_sql}
                            SELECT COUNT(*) AS total_items
                            FROM filtered
                            """
                        ),
                        base_params,
                    ).scalar_one()
                )
                if total_items == 0:
                    return self._get_inventory_status_fallback(
                        store_id=store_id,
                        page=page,
                        page_size=page_size,
                        status_filters=status_filters,
                    )

                rows = (
                    conn.execute(
                        text(
                            f"""
                            {base_cte_sql}
                            SELECT
                                f.item_cd,
                                f.item_nm,
                                COALESCE(cat.category, psl.item_group) AS item_group,
                                f.stk_avg,
                                f.sal_avg,
                                f.ord_avg,
                                f.stk_rt,
                                f.is_stockout,
                                f.stockout_hour,
                                f.status
                            FROM filtered AS f
                            LEFT JOIN LATERAL (
                                SELECT MAX(COALESCE(NULLIF(TRIM(CAST(mic.category AS TEXT)), ''), '기타')) AS category
                                FROM mart_item_category_master mic
                                WHERE (
                                    COALESCE(NULLIF(TRIM(CAST(mic.item_cd AS TEXT)), ''), '') <> ''
                                    AND COALESCE(NULLIF(TRIM(CAST(mic.item_cd AS TEXT)), ''), '') =
                                        COALESCE(NULLIF(TRIM(CAST(f.item_cd AS TEXT)), ''), '')
                                )
                                OR COALESCE(NULLIF(TRIM(CAST(mic.item_nm AS TEXT)), ''), '') =
                                   COALESCE(NULLIF(TRIM(CAST(f.item_nm AS TEXT)), ''), '')
                                OR (
                                    COALESCE(NULLIF(TRIM(CAST(mic.parent_item_nm AS TEXT)), ''), '') <> ''
                                    AND COALESCE(NULLIF(TRIM(CAST(mic.parent_item_nm AS TEXT)), ''), '') =
                                        COALESCE(NULLIF(TRIM(CAST(f.item_nm AS TEXT)), ''), '')
                                )
                            ) cat ON TRUE
                            LEFT JOIN raw_product_shelf_life AS psl
                                ON psl.item_cd = f.item_cd
                            ORDER BY f.stk_rt ASC, f.item_nm ASC
                            LIMIT :page_size OFFSET :offset
                            """
                        ),
                        {
                            **base_params,
                            "page_size": page_size,
                            "offset": offset,
                        },
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows], total_items, summary_metrics
        except SQLAlchemyError as exc:
            logger.warning("get_inventory_status 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return self._get_inventory_status_fallback(
                store_id=store_id,
                page=page,
                page_size=page_size,
                status_filters=status_filters,
            )

    async def get_registration_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(
                        store_id=store_id, date_from=date_from, date_to=date_to
                    )
                    recent_filter_clause, recent_params = self._build_history_filters(
                        store_id=store_id,
                        date_from=date_from or datetime.now().date().isoformat(),
                        date_to=date_to,
                    )
                    summary = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                COUNT(*) AS total,
                                COALESCE(SUM(qty), 0) AS total_registered_qty
                            FROM production_registrations
                            {filter_clause}
                            """
                            ),
                            params,
                        )
                        .mappings()
                        .one()
                    )
                    latest = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                sku_id,
                                qty,
                                registered_by,
                                COALESCE(feedback_type, 'chance_loss_reduced') AS feedback_type,
                                COALESCE(feedback_message, '재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.') AS feedback_message,
                                TO_CHAR(registered_at, 'YYYY-MM-DD HH24:MI:SS') AS registered_at,
                                store_id
                            FROM production_registrations
                            {filter_clause}
                            ORDER BY registered_at DESC
                            LIMIT 1
                            """
                            ),
                            params,
                        )
                        .mappings()
                        .first()
                    )
                    recent_registered_by = (
                        connection.execute(
                            text(
                                f"""
                            SELECT registered_by
                            FROM production_registrations
                            {filter_clause}
                            GROUP BY registered_by
                            ORDER BY MAX(registered_at) DESC
                            LIMIT 5
                            """
                            ),
                            params,
                        )
                        .scalars()
                        .all()
                    )
                    recent_7d_summary = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                COUNT(*) AS recent_registration_count_7d,
                                COALESCE(SUM(qty), 0) AS recent_registered_qty_7d,
                                COUNT(DISTINCT sku_id) AS affected_sku_count
                            FROM production_registrations
                            {recent_filter_clause}
                            """
                            ),
                            recent_params,
                        )
                        .mappings()
                        .one()
                    )
                    return {
                        "total": int(summary["total"]),
                        "latest": dict(latest) if latest else None,
                        "total_registered_qty": int(summary["total_registered_qty"]),
                        "recent_registered_by": list(recent_registered_by),
                        "recent_registration_count_7d": int(
                            recent_7d_summary["recent_registration_count_7d"]
                        ),
                        "recent_registered_qty_7d": int(
                            recent_7d_summary["recent_registered_qty_7d"]
                        ),
                        "affected_sku_count": int(recent_7d_summary["affected_sku_count"]),
                        "summary_status": "active" if int(summary["total"]) > 0 else "empty",
                        "filtered_store_id": store_id,
                        "filtered_date_from": date_from,
                        "filtered_date_to": date_to,
                    }
            except SQLAlchemyError:
                return {
                    "total": 0,
                    "latest": None,
                    "total_registered_qty": 0,
                    "recent_registered_by": [],
                    "recent_registration_count_7d": 0,
                    "recent_registered_qty_7d": 0,
                    "affected_sku_count": 0,
                    "summary_status": "empty",
                    "filtered_store_id": store_id,
                    "filtered_date_from": date_from,
                    "filtered_date_to": date_to,
                }
        return {
            "total": 0,
            "latest": None,
            "total_registered_qty": 0,
            "recent_registered_by": [],
            "recent_registration_count_7d": 0,
            "recent_registered_qty_7d": 0,
            "affected_sku_count": 0,
            "summary_status": "empty",
            "filtered_store_id": store_id,
            "filtered_date_from": date_from,
            "filtered_date_to": date_to,
        }

    def get_fifo_lot_summary(
        self,
        store_id: str,
        lot_type: str | None = None,
        page: int = 1,
        page_size: int = 20,
        date: str | None = None,
    ) -> tuple[list[dict], int]:
        """점포별 FIFO Lot 품목 요약 조회.

        품목·Lot 유형별로 생산/소진/폐기/잔여 수량을 집계한다.
        date 파라미터는 조회 기준일이며, 해당 일자(lot_date) 데이터만 집계한다.
        date 파라미터가 없으면 KST 오늘 날짜를 기본값으로 사용한다.
        """
        mart_result = self._get_fifo_lot_summary_from_inventory_mart(
            store_id=store_id,
            lot_type=lot_type,
            page=page,
            page_size=page_size,
            date=date,
        )
        if mart_result is not None and mart_result[1] > 0:
            return mart_result
        if not self.engine or not has_table(self.engine, "inventory_fifo_lots"):
            return [], 0
        try:
            _, _, month_start, previous_day = self._resolve_fifo_month_window(date)
            if previous_day is None:
                return [], 0

            params: dict = {
                "store_id": store_id,
                "month_start_date": month_start.isoformat(),
                "previous_day_date": previous_day.isoformat(),
            }

            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                item_cd,
                                item_nm,
                                lot_type,
                                MAX(shelf_life_days)                                          AS shelf_life_days,
                                MAX(lot_date)                                                 AS last_lot_date,
                                SUM(initial_qty)                                              AS total_initial_qty,
                                SUM(consumed_qty)                                             AS total_consumed_qty,
                                SUM(wasted_qty)                                               AS total_wasted_qty,
                                SUM(CASE WHEN status = 'active'
                                    THEN initial_qty - consumed_qty ELSE 0 END)               AS active_remaining_qty,
                                COUNT(*) FILTER (WHERE status = 'active')                     AS active_lot_count,
                                COUNT(*) FILTER (WHERE status = 'sold_out')                   AS sold_out_lot_count,
                                COUNT(*) FILTER (WHERE status = 'expired')                    AS expired_lot_count
                            FROM inventory_fifo_lots
                            WHERE masked_stor_cd = :store_id
                              AND lot_date BETWEEN :month_start_date AND :previous_day_date
                            GROUP BY item_cd, item_nm, lot_type
                            ORDER BY total_wasted_qty DESC, item_nm
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
            normalized_rows, total = self._normalize_fifo_lot_rows(
                store_id=store_id,
                rows=[dict(r) for r in rows],
                lot_type=lot_type,
                page=page,
                page_size=page_size,
                month_start_date=month_start.isoformat(),
                previous_day_date=previous_day.isoformat(),
            )
            return normalized_rows, total
        except SQLAlchemyError as exc:
            logger.warning(
                "get_fifo_lot_summary 쿼리 실패: store_id=%s error=%s",
                store_id,
                exc,
            )
            return [], 0
