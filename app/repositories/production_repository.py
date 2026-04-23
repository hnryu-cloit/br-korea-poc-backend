from __future__ import annotations

import logging
from datetime import date as date_type
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table
from app.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


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
        self, store_id: str, page: int, page_size: int
    ) -> tuple[list[dict], int, dict]:
        offset = max(0, (page - 1) * page_size)
        inventory_rows = [
            row
            for row in self._fetch_recent_inventory_rows(store_id=store_id, rank_limit=1)
            if int(row.get("dr") or 0) == 1
        ]
        if not inventory_rows:
            return self._get_inventory_status_fallback(
                store_id=store_id,
                page=page,
                page_size=page_size,
            )

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

        rows.sort(key=lambda item: (str(item.get("item_cd") or ""), str(item.get("item_nm") or "")))
        summary_metrics = self._summarize_inventory_rows(rows)
        return rows[offset : offset + page_size], len(rows), summary_metrics

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
                n_days = max(1, len(bucket["dates"]))  # type: ignore[arg-type]
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
        if current <= 0 and production_qty > 0:
            current = production_qty

        demand_baseline = max(sale_qty, order_confirm_qty, hourly_sale_qty)
        forecast = demand_baseline
        if forecast <= 0 and production_qty > 0:
            forecast = min(production_qty, max(4, current + max(4, current // 2)))
        if forecast <= 0:
            forecast = max(0, current // 2)
        current = max(0, current)
        forecast = max(0, forecast)

        order_pressure = order_confirm_qty >= max(1, int(round(forecast * 0.8)))
        velocity_pressure = hourly_sale_qty >= max(1, int(round(forecast * 0.8)))

        if forecast <= 0:
            status = "safe"
        elif current <= forecast or (order_pressure and current <= int(round(forecast * 1.1))):
            status = "danger"
        elif current <= int(round(forecast * 1.5)) or velocity_pressure:
            status = "warning"
        else:
            status = "safe"
        if status == "safe":
            recommended = 0
        else:
            gap = max(forecast - current, 0)
            buffer_qty = max(4, int(round(forecast * 0.2)))
            recommended = self._clamp_recommended_qty(current, forecast, gap + buffer_qty)

        prod1_qty = production_qty if production else max(8, current + 8)
        if prod1_qty <= 0:
            prod1_qty = max(8, current + 8)
        prod2_qty = secondary_qty if secondary else max(recommended, current)
        if prod2_qty <= 0:
            prod2_qty = max(recommended, current)
        if status != "safe":
            prod2_qty = max(prod2_qty, recommended)

        risk_score = max(forecast - current, 0)
        return {
            "sku_id": item_cd,
            "name": item_nm,
            "current": current,
            "forecast": forecast,
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
        ranked_rows.sort(
            key=lambda row: (-int(row["_risk_score"]), -int(row["forecast"]), str(row["name"]))
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
        combined_keys = active_keys or (
            set(production_map)
            | set(secondary_map)
            | set(stock_map)
            | set(sale_map)
            | set(order_confirm_map)
            | set(hourly_sale_map)
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

    async def list_items(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
    ) -> list[dict]:
        production_map: dict[str, dict[str, object]] = {}
        secondary_map: dict[str, dict[str, object]] = {}
        stock_map: dict[str, dict[str, object]] = {}
        sale_map: dict[str, dict[str, object]] = {}
        order_confirm_map: dict[str, dict[str, object]] = {}
        hourly_sale_map: dict[str, dict[str, object]] = {}

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
                reference_date=business_date,
            )
            secondary_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty_2", "reprod_qty", "prod_qty_3"),
                store_id=store_id,
                window_days=28,
                reference_date=business_date,
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

        if self.engine and has_table(self.engine, "raw_order_extract"):
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

        active_keys = set(sale_map)
        if not active_keys:
            active_keys = set(stock_map)
        if not active_keys:
            active_keys = set(order_confirm_map) | set(hourly_sale_map)

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

    def get_disuse_and_cost_latest_rows(self, store_id: str) -> list[dict]:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH latest_date AS (
                                SELECT MAX(stock_dt) AS stock_dt
                                FROM raw_inventory_extract
                                WHERE masked_stor_cd = :store_id
                            )
                            SELECT
                                COALESCE(item_cd, item_nm) AS item_cd,
                                item_nm,
                                SUM(COALESCE(NULLIF(TRIM(disuse_qty), '')::numeric, 0)) AS total_disuse_qty,
                                AVG(COALESCE(NULLIF(TRIM(cost), '')::numeric, 0)) AS avg_cost
                            FROM raw_inventory_extract r
                            JOIN latest_date d ON r.stock_dt = d.stock_dt
                            WHERE r.masked_stor_cd = :store_id
                            GROUP BY COALESCE(item_cd, item_nm), item_nm
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "get_disuse_and_cost_latest_rows 쿼리 실패: store_id=%s error=%s",
                store_id,
                exc,
            )
            return []

    def get_monthly_disuse_rows(
        self,
        store_id: str,
        date_from: str,
        date_to: str,
    ) -> list[dict]:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                COALESCE(item_cd, item_nm) AS item_cd,
                                item_nm,
                                SUM(COALESCE(NULLIF(TRIM(disuse_qty), '')::numeric, 0)) AS total_disuse_qty,
                                SUM(
                                    COALESCE(NULLIF(TRIM(disuse_qty), '')::numeric, 0)
                                    * COALESCE(NULLIF(TRIM(cost), '')::numeric, 0)
                                ) AS total_disuse_amount,
                                AVG(COALESCE(NULLIF(TRIM(cost), '')::numeric, 0)) AS avg_cost
                            FROM raw_inventory_extract
                            WHERE masked_stor_cd = :store_id
                              AND stock_dt >= :date_from
                              AND stock_dt <= :date_to
                            GROUP BY COALESCE(item_cd, item_nm), item_nm
                            """
                        ),
                        {"store_id": store_id, "date_from": date_from, "date_to": date_to},
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows]
        except SQLAlchemyError as exc:
            logger.warning(
                "get_monthly_disuse_rows 쿼리 실패: store_id=%s date_from=%s date_to=%s error=%s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            return []

    def get_inventory_status(
        self, store_id: str | None = None, page: int = 1, page_size: int = 10
    ) -> tuple[list[dict], int, dict]:
        if not self.engine or not store_id:
            return [], 0, {}
        try:
            offset = max(0, (page - 1) * page_size)
            with self.engine.connect() as conn:
                total_items = int(
                    conn.execute(
                        text(
                            """
                            SELECT COUNT(DISTINCT item_cd) AS total_items
                            FROM core_stock_rate
                            WHERE masked_stor_cd = :store_id
                            """
                        ),
                        {"store_id": store_id},
                    ).scalar_one()
                )
                summary_row = conn.execute(
                    text(
                        """
                        WITH latest AS (
                            SELECT DISTINCT ON (item_cd)
                                stk_rt, is_stockout
                            FROM core_stock_rate
                            WHERE masked_stor_cd = :store_id
                            ORDER BY item_cd, prc_dt DESC
                        )
                        SELECT
                            COUNT(*) FILTER (WHERE stk_rt < 0 OR is_stockout)  AS shortage_count,
                            COUNT(*) FILTER (WHERE stk_rt >= 0.35 AND NOT is_stockout) AS excess_count,
                            COUNT(*) FILTER (WHERE stk_rt >= 0 AND stk_rt < 0.35 AND NOT is_stockout) AS normal_count,
                            AVG(stk_rt) AS avg_stock_rate
                        FROM latest
                        """
                    ),
                    {"store_id": store_id},
                ).mappings().one()
                summary_metrics = dict(summary_row)
                if total_items == 0:
                    return self._get_inventory_status_fallback(
                        store_id=store_id,
                        page=page,
                        page_size=page_size,
                    )

                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT DISTINCT ON (sr.item_cd)
                                sr.item_cd,
                                sr.item_nm,
                                sr.stk_avg,
                                sr.sal_avg,
                                sr.ord_avg,
                                sr.stk_rt,
                                sr.is_stockout,
                                st.stockout_hour
                            FROM core_stock_rate sr
                            LEFT JOIN core_stockout_time st
                                ON sr.masked_stor_cd = st.masked_stor_cd
                               AND sr.item_cd = st.item_cd
                               AND sr.prc_dt = st.prc_dt
                            WHERE sr.masked_stor_cd = :store_id
                            ORDER BY sr.item_cd, sr.prc_dt DESC
                            LIMIT :page_size OFFSET :offset
                            """
                        ),
                        {"store_id": store_id, "page_size": page_size, "offset": offset},
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
