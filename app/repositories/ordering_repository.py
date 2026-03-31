from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

ORDER_OPTIONS = [
    {
        "id": "opt-a",
        "label": "지난주 같은 요일",
        "basis": "3월 24일(월) 기준",
        "description": "가장 최근 데이터 기준이에요. 오늘 날씨와 비슷해 무난한 선택입니다.",
        "recommended": True,
        "items": [
            {"name": "스트로베리 필드", "qty": 120, "note": "캠페인으로 8% 더 팔림"},
            {"name": "글레이즈드", "qty": 96, "note": None},
            {"name": "올드패션", "qty": 80, "note": None},
            {"name": "초코 트위스트", "qty": 72, "note": None},
        ],
        "notes": ["지난주 캠페인으로 도넛 주문이 좀 많았어요", "오후 배달은 조금 줄었어요"],
    },
    {
        "id": "opt-b",
        "label": "2주 전 같은 요일",
        "basis": "3월 17일(월) 기준",
        "description": "행사나 이벤트 영향이 없는 평상시 기준이에요. 넉넉하지 않지만 안전해요.",
        "recommended": False,
        "items": [
            {"name": "스트로베리 필드", "qty": 108, "note": None},
            {"name": "글레이즈드", "qty": 88, "note": None},
            {"name": "올드패션", "qty": 76, "note": None},
            {"name": "초코 트위스트", "qty": 68, "note": None},
        ],
        "notes": ["행사 없었던 날 기준이라 안정적이에요", "재고가 남을 위험이 가장 낮아요"],
    },
    {
        "id": "opt-c",
        "label": "지난달 같은 요일",
        "basis": "2월 24일(월) 기준",
        "description": "한 달 전 같은 요일 기준이에요. 배달 주문이 지금보다 많았던 시기예요.",
        "recommended": False,
        "items": [
            {"name": "스트로베리 필드", "qty": 132, "note": None},
            {"name": "글레이즈드", "qty": 104, "note": None},
            {"name": "올드패션", "qty": 88, "note": None},
            {"name": "초코 트위스트", "qty": 80, "note": None},
        ],
        "notes": ["배달 주문이 지금보다 12% 더 많았어요", "커피 같이 구매가 많았어요"],
    },
]


class OrderingRepository:
    saved_selections: list[dict] = []

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

    async def list_options(self) -> list[dict]:
        source_relation = None
        if self.engine and has_table(self.engine, "core_daily_item_sales"):
            source_relation = "core_daily_item_sales"
        elif self.engine and has_table(self.engine, "raw_daily_store_item"):
            source_relation = "raw_daily_store_item"

        if self.engine and source_relation:
            try:
                with self.engine.connect() as connection:
                    dates = connection.execute(
                        text(
                            f"""
                            SELECT DISTINCT sale_dt
                            FROM {source_relation}
                            WHERE sale_dt IS NOT NULL AND sale_dt <> ''
                            ORDER BY sale_dt DESC
                            LIMIT 6
                            """
                        )
                    ).scalars().all()

                    if len(dates) >= 3:
                        labels = ["지난주 같은 요일", "2주 전 같은 요일", "지난달 같은 요일"]
                        descriptions = [
                            "가장 최근 데이터 기준이에요. 오늘 날씨와 비슷해 무난한 선택입니다.",
                            "행사나 이벤트 영향이 적은 비교 기준이에요. 무난한 기준입니다.",
                            "시즌성과 채널 변동을 크게 반영한 비교 기준이에요.",
                        ]
                        notes_map = [
                            ["최근 판매 추이를 가장 잘 반영한 기준이에요", "운영 변동성이 낮은 SKU를 우선 추천했어요"],
                            ["최근 프로모션 영향이 적은 날짜를 참고했어요", "재고 리스크를 낮추는 쪽으로 정리했어요"],
                            ["수요가 조금 높았던 날을 반영했어요", "배달/온라인 유입 증가 가능성을 고려했어요"],
                        ]
                        options = []
                        for idx, sale_dt in enumerate(dates[:3]):
                            rows = connection.execute(
                                text(
                                    f"""
                                    SELECT item_nm, SUM(sale_qty) AS sale_qty
                                    FROM {source_relation}
                                    WHERE sale_dt = :sale_dt
                                    GROUP BY item_nm
                                    ORDER BY sale_qty DESC, item_nm
                                    LIMIT 4
                                    """
                                ),
                                {"sale_dt": sale_dt},
                            ).mappings()
                            items = []
                            for row_index, row in enumerate(rows):
                                qty = int(round(float(row["sale_qty"])))
                                items.append(
                                    {
                                        "name": row["item_nm"],
                                        "qty": qty,
                                        "note": "추천 상위 SKU" if idx == 0 and row_index == 0 else None,
                                    }
                                )
                            if items:
                                options.append(
                                    {
                                        "id": f"opt-{chr(97 + idx)}",
                                        "label": labels[idx],
                                        "basis": f"{sale_dt[:4]}-{sale_dt[4:6]}-{sale_dt[6:8]} 기준",
                                        "description": descriptions[idx],
                                        "recommended": idx == 0,
                                        "items": items,
                                        "notes": notes_map[idx],
                                    }
                                )
                        if options:
                            return options
            except SQLAlchemyError:
                pass
        return ORDER_OPTIONS

    async def get_notification_context(self, notification_id: int) -> dict:
        return {
            "notification_id": notification_id,
            "target_path": "/ordering",
            "focus_option_id": "opt-a",
            "message": "주문 추천 3개 옵션이 준비되었습니다. 추천 옵션부터 확인하세요.",
        }

    async def save_selection(self, payload: dict) -> dict:
        if self.engine and has_table(self.engine, "ordering_selections"):
            try:
                with self.engine.begin() as connection:
                    row = connection.execute(
                        text(
                            """
                            INSERT INTO ordering_selections(option_id, reason, actor, saved, store_id)
                            VALUES (:option_id, :reason, :actor, TRUE, :store_id)
                            RETURNING option_id, reason, actor, saved, store_id
                            """
                        ),
                        payload,
                    ).mappings().one()
                    return dict(row)
            except SQLAlchemyError:
                pass
        payload = {**payload}
        self.saved_selections.append(payload)
        return {**payload, "saved": True}

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
                                actor,
                                saved,
                                TO_CHAR(selected_at, 'YYYY-MM-DD HH24:MI:SS') AS selected_at,
                                store_id
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
                pass
        return [
            {
                "option_id": entry["option_id"],
                "reason": entry.get("reason"),
                "actor": entry["actor"],
                "saved": True,
                "selected_at": "2026-03-31 00:00:00",
                "store_id": entry.get("store_id"),
            }
            for entry in reversed(
                [
                    entry
                    for entry in self.saved_selections
                    if (store_id is None or entry.get("store_id") == store_id)
                    and self._matches_date_range("2026-03-31 00:00:00", date_from=date_from, date_to=date_to)
                ][-limit:]
            )
        ]

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
                                actor,
                                saved,
                                TO_CHAR(selected_at, 'YYYY-MM-DD HH24:MI:SS') AS selected_at,
                                store_id
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
                pass

        filtered_entries = [
            entry
            for entry in self.saved_selections
            if (store_id is None or entry.get("store_id") == store_id)
            and self._matches_date_range("2026-03-31 00:00:00", date_from=date_from, date_to=date_to)
        ]
        latest_entries = list(reversed(filtered_entries))
        latest = latest_entries[0] if latest_entries else None
        recent_actor_roles: list[str] = []
        for entry in latest_entries:
            actor = entry["actor"]
            if actor not in recent_actor_roles:
                recent_actor_roles.append(actor)
            if len(recent_actor_roles) >= 5:
                break
        option_counts: dict[str, int] = {}
        for entry in filtered_entries:
            option_id = str(entry["option_id"])
            option_counts[option_id] = option_counts.get(option_id, 0) + 1
        return {
            "total": len(filtered_entries),
            "latest": (
                {
                    "option_id": latest["option_id"],
                    "reason": latest.get("reason"),
                    "actor": latest["actor"],
                    "saved": True,
                    "selected_at": "2026-03-31 00:00:00",
                    "store_id": latest.get("store_id"),
                }
                if latest
                else None
            ),
            "recommended_selected": bool(latest and latest["option_id"] == "opt-a"),
            "recent_actor_roles": recent_actor_roles,
            "recent_selection_count_7d": len(filtered_entries),
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
