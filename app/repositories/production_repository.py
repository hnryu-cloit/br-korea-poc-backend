from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
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

    async def list_items(self) -> list[dict]:
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
                        current = max(8, int(round(float(row["sale_qty"]) / 6)))
                        forecast = max(0, current - max(4, int(round(current * 0.75))))
                        status = "danger" if forecast <= 5 else "warning" if forecast <= 12 else "safe"
                        recommended = 0 if status == "safe" else max(12, int(round(float(row["sale_qty"]) / 4)))
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
