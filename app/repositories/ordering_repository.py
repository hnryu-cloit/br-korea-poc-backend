from __future__ import annotations

from datetime import datetime

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table


class OrderingRepository:
    @staticmethod
    def _build_history_filters(store_id: str | None = None, date_from: str | None = None, date_to: str | None = None) -> tuple[str, dict]:
        clauses: list[str] = []
        params: dict[str, str | int | None] = {}
        if store_id:
            clauses.append("store_id = :store_id")
            params["store_id"] = store_id
        if date_from:
            clauses.append("selected_at::date >= CAST(:date_from AS DATE)")
            params["date_from"] = date_from
        if date_to:
            clauses.append("selected_at::date <= CAST(:date_to AS DATE)")
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
    def _format_basis_date(value: object) -> str:
        basis = str(value).strip()
        if len(basis) == 8 and basis.isdigit():
            return f"{basis[:4]}-{basis[4:6]}-{basis[6:8]}"
        return basis.replace("/", "-")[:10]

    def _build_options_from_relation(
        self,
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        quantity_candidates: tuple[str, ...],
        store_id: str | None = None,
    ) -> list[dict]:
        columns = self._table_columns(relation)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, date_candidates)
        item_name_column = self._pick_column(columns, item_name_candidates)
        item_code_column = self._pick_column(columns, item_code_candidates)
        quantity_column = self._pick_column(columns, quantity_candidates)
        if not self.engine or not date_column or not item_name_column or not quantity_column:
            return []

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
        quantity_expr = f"COALESCE(NULLIF(TRIM(CAST({quantity_column} AS TEXT)), '')::numeric, 0)"
        store_filter_sql = ""
        filter_params: dict[str, object] = {}
        if store_id and store_column:
            store_filter_sql = f" AND CAST({store_column} AS TEXT) = :store_id"
            filter_params["store_id"] = store_id

        labels = ["지난주 같은 요일", "2주 전 같은 요일", "지난달 같은 요일"]
        descriptions = [
            "가장 최근 주문 데이터 기준이에요. 오늘 운영에 바로 참고할 수 있습니다.",
            "행사나 이벤트 영향이 적은 비교 기준이에요. 무난한 기준입니다.",
            "시즌성과 채널 변동을 함께 반영한 비교 기준이에요.",
        ]
        notes_map = [
            ["최근 주문 수량이 가장 높은 SKU를 우선 반영했어요", "주문 회전이 빠른 품목 중심으로 정리했어요"],
            ["주문 변동성이 낮은 날짜를 참고했어요", "재고 리스크를 낮추는 쪽으로 정리했어요"],
            ["수요가 조금 높았던 날을 반영했어요", "배달/온라인 유입 증가 가능성을 고려했어요"],
        ]

        try:
            with self.engine.connect() as connection:
                dates = connection.execute(
                    text(
                        f"""
                        SELECT DISTINCT CAST({date_column} AS TEXT) AS date_value
                        FROM {relation}
                        WHERE NULLIF(TRIM(CAST({date_column} AS TEXT)), '') IS NOT NULL
                          {store_filter_sql}
                        ORDER BY date_value DESC
                        LIMIT 6
                        """
                    ),
                    filter_params,
                ).scalars().all()
                options: list[dict] = []
                for date_idx, date_value in enumerate(dates[:3]):
                    rows = connection.execute(
                        text(
                            f"""
                            SELECT
                                {item_name_expr} AS item_name,
                                {item_code_expr} AS item_code,
                                {quantity_expr} AS quantity
                            FROM {relation}
                            WHERE CAST({date_column} AS TEXT) = :date_value
                              {store_filter_sql}
                            """
                        ),
                        {"date_value": str(date_value), **filter_params},
                    ).mappings().all()

                    aggregated: dict[str, dict[str, object]] = {}
                    for row in rows:
                        item_name = str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                        item_code = str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
                        key = item_code or item_name
                        if not key:
                            continue
                        bucket = aggregated.setdefault(
                            key,
                            {
                                "name": item_name or item_code or key,
                                "qty": 0,
                            },
                        )
                        bucket["qty"] = int(bucket["qty"]) + self._safe_int(row["quantity"])

                    if not aggregated:
                        continue

                    sorted_items = sorted(
                        aggregated.values(),
                        key=lambda bucket: (-int(bucket["qty"]), str(bucket["name"])),
                    )
                    items = [
                        {
                            "sku_name": bucket["name"],
                            "quantity": int(bucket["qty"]),
                            "note": "추천 상위 SKU" if date_idx == 0 and idx == 0 else None,
                        }
                        for idx, bucket in enumerate(sorted_items)
                    ]
                    if items:
                        raw_notes = notes_map[date_idx] if date_idx < len(notes_map) else None
                        total_qty = sum(int(b["qty"]) for b in aggregated.values())
                        top_item = sorted_items[0] if sorted_items else None
                        reasoning_metrics: list[dict] = [
                            {"key": "기준일", "value": self._format_basis_date(date_value)},
                            {"key": "총 주문량", "value": f"{total_qty}개"},
                            {"key": "품목 수", "value": f"{len(aggregated)}개 SKU"},
                        ]
                        if top_item:
                            reasoning_metrics.append({"key": "주요 품목", "value": str(top_item["name"])})
                        options.append(
                            {
                                "option_id": f"opt-{chr(97 + date_idx)}",
                                "title": labels[date_idx],
                                "basis": self._format_basis_date(date_value),
                                "description": descriptions[date_idx],
                                "recommended": date_idx == 0,
                                "reasoning_text": raw_notes[0] if raw_notes else "",
                                "reasoning_metrics": reasoning_metrics,
                                "special_factors": raw_notes[1:],
                                "items": items,
                            }
                        )
                return options
        except SQLAlchemyError:
            return []

    async def list_options(self, store_id: str | None = None) -> list[dict]:
        if self.engine and has_table(self.engine, "raw_order_extract"):
            options = self._build_options_from_relation(
                "raw_order_extract",
                ("dlv_dt", "ord_dt", "sale_dt"),
                ("item_nm", "item_name", "product_nm"),
                ("item_cd", "item_code", "sku_id"),
                ("ord_rec_qty", "ord_qty", "confrm_qty"),
                store_id=store_id,
            )
            if options:
                return options

        if self.engine and has_table(self.engine, "core_daily_item_sales"):
            options = self._build_options_from_relation(
                "core_daily_item_sales",
                ("sale_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
            )
            if options:
                return options

        if self.engine and has_table(self.engine, "raw_daily_store_item"):
            options = self._build_options_from_relation(
                "raw_daily_store_item",
                ("sale_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
            )
            if options:
                return options
        return []

    async def get_notification_context(self, notification_id: int) -> dict:
        return {
            "notification_id": notification_id,
            "target_path": "/ordering",
            "focus_option_id": "opt-a",
            "message": "주문 추천 3개 옵션이 준비되었습니다. 추천 옵션부터 확인하세요.",
        }

    async def save_selection(self, payload: dict) -> dict:
        actor_role = payload.get("actor_role", "store_owner")
        if self.engine and has_table(self.engine, "ordering_selections"):
            try:
                with self.engine.begin() as connection:
                    row = connection.execute(
                        text(
                            """
                            INSERT INTO ordering_selections(option_id, reason, actor, saved, store_id)
                            VALUES (:option_id, :reason, :actor_role, TRUE, :store_id)
                            RETURNING option_id, reason, actor AS actor_role, saved, store_id
                            """
                        ),
                        payload,
                    ).mappings().one()
                    return dict(row)
            except SQLAlchemyError:
                return {**payload, "saved": False}
        return {**payload, "saved": False}

    async def list_selection_history(
        self,
        limit: int = 20,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        if self.engine and has_table(self.engine, "ordering_selections"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(store_id=store_id, date_from=date_from, date_to=date_to)
                    rows = connection.execute(
                        text(
                            f"""
                            SELECT
                                option_id,
                                reason,
                                actor AS actor_role,
                                store_id,
                                TO_CHAR(selected_at, 'YYYY-MM-DD HH24:MI:SS') AS selected_at
                            FROM ordering_selections
                            {filter_clause}
                            ORDER BY selected_at DESC
                            LIMIT :limit
                            """
                        ),
                        {"limit": limit, **params},
                    ).mappings().all()
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                return []
        return []

    async def get_selection_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        if self.engine and has_table(self.engine, "ordering_selections"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(store_id=store_id, date_from=date_from, date_to=date_to)
                    recent_filter_clause, recent_params = self._build_history_filters(
                        store_id=store_id,
                        date_from=date_from or (datetime.now().date().isoformat()),
                        date_to=date_to,
                    )
                    total = connection.execute(
                        text(f"SELECT COUNT(*) FROM ordering_selections {filter_clause}"),
                        params,
                    ).scalar_one()
                    latest = connection.execute(
                        text(
                            f"""
                            SELECT
                                option_id,
                                reason,
                                actor AS actor_role,
                                store_id,
                                TO_CHAR(selected_at, 'YYYY-MM-DD HH24:MI:SS') AS selected_at
                            FROM ordering_selections
                            {filter_clause}
                            ORDER BY selected_at DESC
                            LIMIT 1
                            """
                        )
                    , params).mappings().first()
                    recent_actor_roles = connection.execute(
                        text(
                            f"""
                            SELECT actor
                            FROM ordering_selections
                            {filter_clause}
                            GROUP BY actor
                            ORDER BY MAX(selected_at) DESC
                            LIMIT 5
                            """
                        )
                    , params).scalars().all()
                    recent_selection_count_7d = connection.execute(
                        text(
                            f"""
                            SELECT COUNT(*)
                            FROM ordering_selections
                            {recent_filter_clause}
                            """
                        )
                    , recent_params).scalar_one()
                    option_counts_rows = connection.execute(
                        text(
                            f"""
                            SELECT option_id, COUNT(*) AS selection_count
                            FROM ordering_selections
                            {filter_clause}
                            GROUP BY option_id
                            ORDER BY option_id
                            """
                        )
                    , params).mappings().all()
                    option_counts = {
                        str(row["option_id"]): int(row["selection_count"]) for row in option_counts_rows
                    }
                    return {
                        "total": int(total),
                        "latest": dict(latest) if latest else None,
                        "recommended_selected": bool(latest and latest["option_id"] == "opt-a"),
                        "recent_actor_roles": list(recent_actor_roles),
                        "recent_selection_count_7d": int(recent_selection_count_7d),
                        "option_counts": option_counts,
                        "summary_status": (
                            "recommended_selected"
                            if latest and latest["option_id"] == "opt-a"
                            else "custom_selected"
                            if latest
                            else "empty"
                        ),
                        "filtered_store_id": store_id,
                        "filtered_date_from": date_from,
                        "filtered_date_to": date_to,
                    }
            except SQLAlchemyError:
                return {
                    "total": 0,
                    "latest": None,
                    "recommended_selected": False,
                    "recent_actor_roles": [],
                    "recent_selection_count_7d": 0,
                    "option_counts": {},
                    "summary_status": "empty",
                    "filtered_store_id": store_id,
                    "filtered_date_from": date_from,
                    "filtered_date_to": date_to,
                }
        return {
            "total": 0,
            "latest": None,
            "recommended_selected": False,
            "recent_actor_roles": [],
            "recent_selection_count_7d": 0,
            "option_counts": {},
            "summary_status": "empty",
            "filtered_store_id": store_id,
            "filtered_date_from": date_from,
            "filtered_date_to": date_to,
        }
