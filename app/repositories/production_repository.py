from __future__ import annotations

from datetime import datetime

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

PRODUCTION_ITEMS = [
    {
        "sku_id": "sku-1",
        "name": "스트로베리 필드",
        "current": 24,
        "forecast": 3,
        "status": "danger",
        "depletion_time": "15:05",
        "recommended": 40,
        "prod1": "08:10 / 52개",
        "prod2": "14:20 / 40개",
    },
    {
        "sku_id": "sku-2",
        "name": "올드패션",
        "current": 18,
        "forecast": 6,
        "status": "danger",
        "depletion_time": "15:22",
        "recommended": 36,
        "prod1": "08:00 / 48개",
        "prod2": "14:10 / 36개",
    },
    {
        "sku_id": "sku-3",
        "name": "크림 필드",
        "current": 12,
        "forecast": 4,
        "status": "danger",
        "depletion_time": "15:18",
        "recommended": 32,
        "prod1": "08:30 / 44개",
        "prod2": "14:30 / 32개",
    },
    {
        "sku_id": "sku-4",
        "name": "글레이즈드",
        "current": 42,
        "forecast": 22,
        "status": "safe",
        "depletion_time": "-",
        "recommended": 0,
        "prod1": "08:05 / 60개",
        "prod2": "14:00 / 48개",
    },
]


class ProductionRepository:
    saved_registrations: list[dict] = []

    @staticmethod
    def _build_history_filters(store_id: str | None = None, date_from: str | None = None, date_to: str | None = None) -> tuple[str, dict]:
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

    @staticmethod
    def _matches_date_range(timestamp_value: str, date_from: str | None = None, date_to: str | None = None) -> bool:
        event_date = datetime.strptime(timestamp_value, "%Y-%m-%d %H:%M:%S").date()
        if date_from and event_date < datetime.strptime(date_from, "%Y-%m-%d").date():
            return False
        if date_to and event_date > datetime.strptime(date_to, "%Y-%m-%d").date():
            return False
        return True

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    def _table_columns(self, table_name: str) -> dict[str, str]:
        if not self.engine:
            return {}
        try:
            return {column["name"].lower(): column["name"] for column in inspect(self.engine).get_columns(table_name)}
        except SQLAlchemyError:
            return {}
        except Exception:
            return {}

    @staticmethod
    def _pick_column(columns: dict[str, str], candidates: tuple[str, ...]) -> str | None:
        for candidate in candidates:
            column_name = columns.get(candidate.lower())
            if column_name:
                return column_name
        return None

    @staticmethod
    def _safe_int(value: object) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _safe_non_negative_int(value: object) -> int:
        return max(0, ProductionRepository._safe_int(value))

    @staticmethod
    def _clamp_recommended_qty(current: int, forecast: int, candidate: int) -> int:
        if forecast <= 0:
            return 0
        lower_bound = max(4, int(round(forecast * 0.2)))
        upper_bound = max(8, int(round(forecast * 1.5)))
        return max(lower_bound, min(candidate, upper_bound))

    @staticmethod
    def _format_basis_date(value: object) -> str:
        basis = str(value).strip()
        if len(basis) == 8 and basis.isdigit():
            return f"{basis[:4]}-{basis[4:6]}-{basis[6:8]}"
        return basis.replace("/", "-")[:10]

    def _fetch_metric_map(
        self,
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        metric_candidates: tuple[str, ...],
    ) -> dict[str, dict[str, object]]:
        columns = self._table_columns(relation)
        date_column = self._pick_column(columns, date_candidates)
        item_name_column = self._pick_column(columns, item_name_candidates)
        item_code_column = self._pick_column(columns, item_code_candidates)
        metric_column = self._pick_column(columns, metric_candidates)
        if not self.engine or not date_column or not item_name_column or not metric_column:
            return {}

        item_name_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_name_column} AS TEXT)), ''), NULLIF(TRIM(CAST({item_code_column} AS TEXT)), ''))"
            if item_code_column
            else f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')"
        )
        item_code_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_code_column} AS TEXT)), ''), NULLIF(TRIM(CAST({item_name_column} AS TEXT)), ''))"
            if item_code_column
            else f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')"
        )
        metric_expr = f"COALESCE(NULLIF(TRIM(CAST({metric_column} AS TEXT)), '')::numeric, 0)"

        try:
            with self.engine.connect() as connection:
                latest_date = connection.execute(
                    text(
                        f"""
                        SELECT DISTINCT CAST({date_column} AS TEXT) AS date_value
                        FROM {relation}
                        WHERE NULLIF(TRIM(CAST({date_column} AS TEXT)), '') IS NOT NULL
                        ORDER BY date_value DESC
                        LIMIT 1
                        """
                    )
                ).scalar_one_or_none()
                if not latest_date:
                    return {}

                rows = connection.execute(
                    text(
                        f"""
                        SELECT
                            {item_name_expr} AS item_name,
                            {item_code_expr} AS item_code,
                            {metric_expr} AS metric_value
                        FROM {relation}
                        WHERE CAST({date_column} AS TEXT) = :date_value
                        """
                    ),
                    {"date_value": str(latest_date)},
                ).mappings().all()
        except SQLAlchemyError:
            return {}

        metric_map: dict[str, dict[str, object]] = {}
        for row in rows:
            item_name = str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
            item_code = str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
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

    def _build_new_items(
        self,
        production_map: dict[str, dict[str, object]],
        secondary_map: dict[str, dict[str, object]],
        stock_map: dict[str, dict[str, object]],
        sale_map: dict[str, dict[str, object]],
    ) -> list[dict]:
        combined_keys = set(production_map) | set(secondary_map) | set(stock_map) | set(sale_map)
        if not combined_keys:
            return []

        ranked_rows: list[dict] = []
        for key in combined_keys:
            production = production_map.get(key, {})
            secondary = secondary_map.get(key, {})
            stock = stock_map.get(key, {})
            sale = sale_map.get(key, {})

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
                or secondary.get("item_nm")
                or key
            )

            stock_qty = self._safe_non_negative_int(stock.get("qty")) if stock else 0
            production_qty = self._safe_non_negative_int(production.get("qty")) if production else 0
            secondary_qty = self._safe_non_negative_int(secondary.get("qty")) if secondary else 0
            sale_qty = self._safe_non_negative_int(sale.get("qty")) if sale else 0

            current = stock_qty if stock else production_qty
            if current <= 0 and not stock and production_qty > 0:
                current = production_qty

            forecast = sale_qty
            if forecast <= 0 and production_qty > 0:
                forecast = min(production_qty, max(4, current + max(4, current // 2)))
            if forecast <= 0:
                forecast = max(0, current // 2)
            current = max(0, current)
            forecast = max(0, forecast)

            if forecast <= 0:
                status = "safe"
            elif current <= forecast:
                status = "danger"
            elif current <= int(round(forecast * 1.5)):
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
            ranked_rows.append(
                {
                    "sku_id": item_cd,
                    "name": item_nm,
                    "current": current,
                    "forecast": forecast,
                    "status": status,
                    "depletion_time": "-",
                    "recommended": recommended,
                    "prod1": f"08:00 / {prod1_qty}개",
                    "prod2": f"14:00 / {prod2_qty}개",
                    "_risk_score": risk_score,
                }
            )

        ranked_rows.sort(key=lambda row: (-int(row["_risk_score"]), -int(row["forecast"]), str(row["name"])))

        for index, row in enumerate(ranked_rows, start=1):
            if row["status"] == "safe":
                row["depletion_time"] = "-"
            else:
                hours_left = max(1, int(round(row["current"] / max(row["forecast"], 1))))
                row["depletion_time"] = f"{min(23, 14 + min(5, hours_left)):02d}:{(10 + index * 7) % 60:02d}"
            row.pop("_risk_score", None)

        return ranked_rows[:4]

    async def list_items(self) -> list[dict]:
        production_map: dict[str, dict[str, object]] = {}
        secondary_map: dict[str, dict[str, object]] = {}
        stock_map: dict[str, dict[str, object]] = {}
        sale_map: dict[str, dict[str, object]] = {}

        if self.engine and has_table(self.engine, "raw_production_extract"):
            production_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty",),
            )
            secondary_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty_2", "reprod_qty", "prod_qty_3"),
            )

        if self.engine and has_table(self.engine, "raw_inventory_extract"):
            stock_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("stock_qty",),
            )
            sale_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
            )

        items = self._build_new_items(production_map, secondary_map, stock_map, sale_map)
        if items:
            return items

        source_relation = None
        if self.engine and has_table(self.engine, "core_hourly_item_sales"):
            source_relation = "core_hourly_item_sales"
        elif self.engine and has_table(self.engine, "raw_daily_store_item_tmzon"):
            source_relation = "raw_daily_store_item_tmzon"

        if self.engine and source_relation:
            try:
                with self.engine.connect() as connection:
                    rows = connection.execute(
                        text(
                            f"""
                            WITH latest_day AS (
                                SELECT MAX(sale_dt) AS sale_dt
                                FROM {source_relation}
                            ),
                            ranked AS (
                                SELECT
                                    item_cd,
                                    item_nm,
                                    SUM(sale_qty) AS sale_qty,
                                    ROW_NUMBER() OVER (
                                        ORDER BY SUM(sale_qty) DESC, item_nm
                                    ) AS row_num
                                FROM {source_relation}
                                WHERE sale_dt = (SELECT sale_dt FROM latest_day)
                                GROUP BY item_cd, item_nm
                            )
                            SELECT item_cd, item_nm, sale_qty
                            FROM ranked
                            WHERE row_num <= 4
                            ORDER BY sale_qty DESC, item_nm
                            """
                        )
                    ).mappings()

                    items = []
                    for index, row in enumerate(rows, start=1):
                        sale_qty = max(0, self._safe_int(row["sale_qty"]))
                        current = max(0, int(round(sale_qty / 6)))
                        forecast = max(0, current - max(4, int(round(current * 0.75))))
                        status = "danger" if forecast <= 5 else "warning" if forecast <= 12 else "safe"
                        recommended = (
                            0
                            if status == "safe"
                            else self._clamp_recommended_qty(
                                current,
                                forecast,
                                max(0, int(round(sale_qty / 4))),
                            )
                        )
                        items.append(
                            {
                                "sku_id": str(row["item_cd"] or f"sku-{index}"),
                                "name": row["item_nm"],
                                "current": current,
                                "forecast": forecast,
                                "status": status,
                                "depletion_time": "-" if status == "safe" else f"1{4 + index}:1{index}",
                                "recommended": recommended,
                                "prod1": f"08:0{index} / {recommended + 12 if recommended else current + 8}개",
                                "prod2": f"14:1{index} / {recommended if recommended else current}개",
                            }
                        )
                    if items:
                        return items
            except SQLAlchemyError:
                pass
        return PRODUCTION_ITEMS

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
                pass
        self.saved_registrations.append(payload)
        return payload

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
                    filter_clause, params = self._build_history_filters(store_id=store_id, date_from=date_from, date_to=date_to)
                    rows = connection.execute(
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
                    ).mappings().all()
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                pass
        return [
            {
                "sku_id": entry["sku_id"],
                "qty": entry["qty"],
                "registered_by": entry["registered_by"],
                "feedback_type": "chance_loss_reduced",
                "feedback_message": "재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
                "registered_at": "2026-03-31 00:00:00",
                "store_id": entry.get("store_id"),
            }
            for entry in reversed(
                [
                    entry
                    for entry in self.saved_registrations
                    if (store_id is None or entry.get("store_id") == store_id)
                    and self._matches_date_range("2026-03-31 00:00:00", date_from=date_from, date_to=date_to)
                ][-limit:]
            )
        ]

    async def get_registration_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(store_id=store_id, date_from=date_from, date_to=date_to)
                    recent_filter_clause, recent_params = self._build_history_filters(
                        store_id=store_id,
                        date_from=date_from or datetime.now().date().isoformat(),
                        date_to=date_to,
                    )
                    summary = connection.execute(
                        text(
                            f"""
                            SELECT
                                COUNT(*) AS total,
                                COALESCE(SUM(qty), 0) AS total_registered_qty
                            FROM production_registrations
                            {filter_clause}
                            """
                        )
                    , params).mappings().one()
                    latest = connection.execute(
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
                        )
                    , params).mappings().first()
                    recent_registered_by = connection.execute(
                        text(
                            f"""
                            SELECT registered_by
                            FROM production_registrations
                            {filter_clause}
                            GROUP BY registered_by
                            ORDER BY MAX(registered_at) DESC
                            LIMIT 5
                            """
                        )
                    , params).scalars().all()
                    recent_7d_summary = connection.execute(
                        text(
                            f"""
                            SELECT
                                COUNT(*) AS recent_registration_count_7d,
                                COALESCE(SUM(qty), 0) AS recent_registered_qty_7d,
                                COUNT(DISTINCT sku_id) AS affected_sku_count
                            FROM production_registrations
                            {recent_filter_clause}
                            """
                        )
                    , recent_params).mappings().one()
                    return {
                        "total": int(summary["total"]),
                        "latest": dict(latest) if latest else None,
                        "total_registered_qty": int(summary["total_registered_qty"]),
                        "recent_registered_by": list(recent_registered_by),
                        "recent_registration_count_7d": int(recent_7d_summary["recent_registration_count_7d"]),
                        "recent_registered_qty_7d": int(recent_7d_summary["recent_registered_qty_7d"]),
                        "affected_sku_count": int(recent_7d_summary["affected_sku_count"]),
                        "summary_status": "active" if int(summary["total"]) > 0 else "empty",
                        "filtered_store_id": store_id,
                        "filtered_date_from": date_from,
                        "filtered_date_to": date_to,
                    }
            except SQLAlchemyError:
                pass

        filtered_entries = [
            entry
            for entry in self.saved_registrations
            if (store_id is None or entry.get("store_id") == store_id)
            and self._matches_date_range("2026-03-31 00:00:00", date_from=date_from, date_to=date_to)
        ]
        latest_entries = list(reversed(filtered_entries))
        latest = latest_entries[0] if latest_entries else None
        recent_registered_by: list[str] = []
        total_registered_qty = 0
        affected_sku_ids: set[str] = set()
        for entry in filtered_entries:
            total_registered_qty += int(entry["qty"])
            affected_sku_ids.add(str(entry["sku_id"]))
        for entry in latest_entries:
            actor = entry["registered_by"]
            if actor not in recent_registered_by:
                recent_registered_by.append(actor)
            if len(recent_registered_by) >= 5:
                break
        return {
            "total": len(filtered_entries),
            "latest": (
                {
                    "sku_id": latest["sku_id"],
                    "qty": latest["qty"],
                    "registered_by": latest["registered_by"],
                    "feedback_type": "chance_loss_reduced",
                    "feedback_message": "재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
                    "registered_at": "2026-03-31 00:00:00",
                    "store_id": latest.get("store_id"),
                }
                if latest
                else None
            ),
            "total_registered_qty": total_registered_qty,
            "recent_registered_by": recent_registered_by,
            "recent_registration_count_7d": len(filtered_entries),
            "recent_registered_qty_7d": total_registered_qty,
            "affected_sku_count": len(affected_sku_ids),
            "summary_status": "active" if filtered_entries else "empty",
            "filtered_store_id": store_id,
            "filtered_date_from": date_from,
            "filtered_date_to": date_to,
        }
