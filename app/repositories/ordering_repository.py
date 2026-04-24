from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

_OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_DEFAULT_WEATHER_COORD = (37.5665, 126.9780)  # 서울
_SIDO_COORDINATES: dict[str, tuple[float, float]] = {
    "서울": (37.5665, 126.9780),
    "서울특별시": (37.5665, 126.9780),
    "경기": (37.4138, 127.5183),
    "경기도": (37.4138, 127.5183),
    "인천": (37.4563, 126.7052),
    "인천광역시": (37.4563, 126.7052),
    "강원": (37.8228, 128.1555),
    "강원도": (37.8228, 128.1555),
    "충북": (36.6358, 127.4914),
    "충청북도": (36.6358, 127.4914),
    "충남": (36.6588, 126.6728),
    "충청남도": (36.6588, 126.6728),
    "대전": (36.3504, 127.3845),
    "대전광역시": (36.3504, 127.3845),
    "세종": (36.4800, 127.2890),
    "세종특별자치시": (36.4800, 127.2890),
    "전북": (35.7175, 127.1530),
    "전라북도": (35.7175, 127.1530),
    "전남": (34.8679, 126.9910),
    "전라남도": (34.8679, 126.9910),
    "광주": (35.1595, 126.8526),
    "광주광역시": (35.1595, 126.8526),
    "경북": (36.4919, 128.8889),
    "경상북도": (36.4919, 128.8889),
    "경남": (35.4606, 128.2132),
    "경상남도": (35.4606, 128.2132),
    "대구": (35.8714, 128.6014),
    "대구광역시": (35.8714, 128.6014),
    "울산": (35.5384, 129.3114),
    "울산광역시": (35.5384, 129.3114),
    "부산": (35.1796, 129.0756),
    "부산광역시": (35.1796, 129.0756),
    "제주": (33.4996, 126.5312),
    "제주특별자치도": (33.4996, 126.5312),
}
_WEATHER_CODE_LABELS: dict[int, str] = {
    0: "맑음",
    1: "대체로 맑음",
    2: "구름 조금",
    3: "흐림",
    45: "안개",
    48: "안개",
    51: "약한 이슬비",
    53: "이슬비",
    55: "강한 이슬비",
    61: "약한 비",
    63: "비",
    65: "강한 비",
    66: "약한 어는비",
    67: "어는비",
    71: "약한 눈",
    73: "눈",
    75: "강한 눈",
    77: "진눈깨비",
    80: "약한 소나기",
    81: "소나기",
    82: "강한 소나기",
    85: "약한 눈소나기",
    86: "눈소나기",
    95: "뇌우",
    96: "우박 동반 뇌우",
    99: "강한 우박 동반 뇌우",
}


class OrderingRepository:
    _DEFAULT_HISTORY_ORDER_HOUR = 12

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine
        self.prefer_store_cache = True
        self.store_cache_root = Path(__file__).resolve().parents[2] / "data" / "store_cache"

    @staticmethod
    def _normalize_yyyymmdd(value: str | None) -> str | None:
        if value in (None, ""):
            return None
        text_value = str(value).strip().replace("-", "")
        return text_value if len(text_value) == 8 and text_value.isdigit() else None

    def _resolve_store_cache_db_path(self, store_id: str | None) -> Path | None:
        if not self.prefer_store_cache or not store_id:
            return None
        candidate = self.store_cache_root / f"{store_id.lower()}_lite.db"
        return candidate if candidate.exists() else None

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

    @classmethod
    def _build_history_visibility_filter(
        cls,
        reference_datetime: datetime | None,
    ) -> tuple[str | None, bool]:
        if reference_datetime is None:
            return None, True
        reference_date = reference_datetime.strftime("%Y%m%d")
        include_same_day = reference_datetime.hour >= cls._DEFAULT_HISTORY_ORDER_HOUR
        return reference_date, include_same_day

    def is_known_store(self, store_id: str) -> bool:
        if not self.engine or not store_id:
            return False
        try:
            with self.engine.connect() as connection:
                exists_in_master = connection.execute(
                    text(
                        """
                        SELECT 1
                        FROM raw_store_master
                        WHERE masked_stor_cd = :store_id
                        LIMIT 1
                        """
                    ),
                    {"store_id": store_id},
                ).scalar_one_or_none()
                if exists_in_master is not None:
                    return True

                # 점포 마스터 누락 상황에서도 주문 원천 데이터가 있으면 유효 점포로 간주
                exists_in_order = connection.execute(
                    text(
                        """
                        SELECT 1
                        FROM raw_order_extract
                        WHERE masked_stor_cd = :store_id
                        LIMIT 1
                        """
                    ),
                    {"store_id": store_id},
                ).scalar_one_or_none()
                return exists_in_order is not None
        except SQLAlchemyError:
            return False

    def _get_history_filtered_from_store_cache(
        self,
        *,
        cache_path: Path,
        store_id: str | None = None,
        limit: int = 100,
        date_from: str | None = None,
        date_to: str | None = None,
        item_nm: str | None = None,
        is_auto: bool | None = None,
        reference_datetime: datetime | None = None,
    ) -> dict:
        date_from_norm = self._normalize_yyyymmdd(date_from)
        date_to_norm = self._normalize_yyyymmdd(date_to)
        reference_date_norm, include_same_day = self._build_history_visibility_filter(reference_datetime)

        where_clauses = ["1=1"]
        params: list[object] = []
        if store_id:
            where_clauses.append("store_id = ?")
            params.append(store_id)
        if date_from_norm:
            where_clauses.append("REPLACE(CAST(dlv_dt AS TEXT), '-', '') >= ?")
            params.append(date_from_norm)
        if date_to_norm:
            where_clauses.append("REPLACE(CAST(dlv_dt AS TEXT), '-', '') <= ?")
            params.append(date_to_norm)
        if reference_date_norm:
            where_clauses.append(
                f"REPLACE(CAST(dlv_dt AS TEXT), '-', '') {'<=' if include_same_day else '<'} ?"
            )
            params.append(reference_date_norm)
        if item_nm:
            where_clauses.append("CAST(item_nm AS TEXT) LIKE ?")
            params.append(f"%{item_nm.strip()}%")
        if is_auto is not None:
            where_clauses.append("CAST(auto_ord_yn AS TEXT) = ?")
            params.append("1" if is_auto else "0")

        where_sql = " AND ".join(where_clauses)
        try:
            with sqlite3.connect(cache_path) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    f"""
                    SELECT
                        CAST(item_nm AS TEXT) AS item_nm,
                        CAST(dlv_dt AS TEXT) AS dlv_dt,
                        ROUND(SUM(COALESCE(ord_qty, 0))) AS ord_qty,
                        ROUND(SUM(COALESCE(confrm_qty, 0))) AS confrm_qty,
                        MAX(CAST(auto_ord_yn AS TEXT)) AS auto_ord_yn,
                        MAX(CAST(ord_grp_nm AS TEXT)) AS ord_grp_nm
                    FROM order_history
                    WHERE {where_sql}
                    GROUP BY CAST(dlv_dt AS TEXT), CAST(item_nm AS TEXT)
                    ORDER BY REPLACE(CAST(dlv_dt AS TEXT), '-', '') DESC, CAST(item_nm AS TEXT)
                    LIMIT ?
                    """,
                    [*params, limit],
                ).fetchall()
        except sqlite3.Error:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}
        return self._build_history_response(list(rows))

    def _resolve_store_sido(self, store_id: str | None) -> str:
        if not self.engine or not store_id or not has_table(self.engine, "raw_store_master"):
            return "서울"
        try:
            with self.engine.connect() as connection:
                raw_sido = connection.execute(
                    text(
                        """
                        SELECT NULLIF(TRIM(CAST(sido AS TEXT)), '')
                        FROM raw_store_master
                        WHERE masked_stor_cd = :store_id
                        LIMIT 1
                        """
                    ),
                    {"store_id": store_id},
                ).scalar_one_or_none()
                if raw_sido:
                    return str(raw_sido)
        except SQLAlchemyError:
            return "서울"
        return "서울"

    @staticmethod
    def _classify_weather_type(avg_temp_c: float | int | None, precipitation_mm: float | int | None) -> str:
        temp = float(avg_temp_c or 0.0)
        precipitation = float(precipitation_mm or 0.0)
        if precipitation >= 5:
            return "눈" if temp <= 0 else "비"
        if precipitation > 0:
            return "진눈깨비" if temp <= 1 else "흐리고 비"
        if temp <= 0:
            return "흐림"
        return "맑음"

    def _get_weather_for_reference_date(
        self,
        *,
        store_id: str | None,
        reference_date: str | None,
    ) -> dict[str, object] | None:
        normalized_date = self._normalize_yyyymmdd(reference_date)
        sido = self._resolve_store_sido(store_id=store_id)
        if (
            not self.engine
            or not normalized_date
            or not has_table(self.engine, "raw_weather_daily")
        ):
            return None
        try:
            with self.engine.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            """
                            SELECT weather_dt, sido, avg_temp_c, precipitation_mm
                            FROM raw_weather_daily
                            WHERE weather_dt = :reference_date
                              AND sido = :sido
                            LIMIT 1
                            """
                        ),
                        {"reference_date": normalized_date, "sido": sido},
                    )
                    .mappings()
                    .first()
                )
        except SQLAlchemyError:
            return None
        if not row:
            return None
        avg_temp_c = float(row["avg_temp_c"] or 0.0)
        precipitation_mm = float(row["precipitation_mm"] or 0.0)
        return {
            "region": str(row["sido"] or sido),
            "forecast_date": f"{normalized_date[:4]}-{normalized_date[4:6]}-{normalized_date[6:8]}",
            "weather_type": self._classify_weather_type(avg_temp_c, precipitation_mm),
            "max_temperature_c": int(round(avg_temp_c)),
            "min_temperature_c": int(round(avg_temp_c)),
            "precipitation_probability": int(round(precipitation_mm)),
        }

    async def get_weather_forecast(
        self,
        store_id: str | None = None,
        reference_date: str | None = None,
    ) -> dict[str, object] | None:
        weather_for_reference = self._get_weather_for_reference_date(
            store_id=store_id,
            reference_date=reference_date,
        )
        if weather_for_reference is not None:
            return weather_for_reference

        sido = self._resolve_store_sido(store_id=store_id)
        latitude, longitude = _SIDO_COORDINATES.get(sido, _DEFAULT_WEATHER_COORD)
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                response = await client.get(
                    _OPEN_METEO_FORECAST_URL,
                    params={
                        "latitude": latitude,
                        "longitude": longitude,
                        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
                        "forecast_days": 1,
                        "timezone": "Asia/Seoul",
                    },
                )
                response.raise_for_status()
                daily = (response.json() or {}).get("daily") or {}
                date = (daily.get("time") or [None])[0]
                max_temp = (daily.get("temperature_2m_max") or [None])[0]
                min_temp = (daily.get("temperature_2m_min") or [None])[0]
                rain_prob = (daily.get("precipitation_probability_max") or [None])[0]
                weather_code = (daily.get("weather_code") or [None])[0]
                if not date:
                    return None
                weather_label = _WEATHER_CODE_LABELS.get(int(weather_code), "날씨")
                return {
                    "region": sido,
                    "forecast_date": str(date),
                    "weather_type": weather_label,
                    "max_temperature_c": None if max_temp is None else int(round(float(max_temp))),
                    "min_temperature_c": None if min_temp is None else int(round(float(min_temp))),
                    "precipitation_probability": None if rain_prob is None else int(round(float(rain_prob))),
                }
        except (httpx.HTTPError, ValueError, TypeError):
            return None

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
    def _is_auto_order_flag(value: object | None) -> bool:
        normalized = str(value or "").strip().lower()
        return normalized in {"1", "y", "yes", "true", "t", "auto", "automatic", "자동"}

    def _build_history_response(self, rows: list[object]) -> dict:
        items = [
            {
                "item_nm": str(row["item_nm"]) if row["item_nm"] is not None else "",
                "dlv_dt": str(row["dlv_dt"]) if row["dlv_dt"] is not None else None,
                "ord_qty": int(row["ord_qty"]) if row["ord_qty"] is not None else None,
                "confrm_qty": int(row["confrm_qty"]) if row["confrm_qty"] is not None else None,
                "is_auto": self._is_auto_order_flag(row["auto_ord_yn"]),
                "ord_grp_nm": str(row["ord_grp_nm"]) if row["ord_grp_nm"] is not None else None,
            }
            for row in rows
        ]
        total_count = len(items)
        auto_count = sum(1 for item in items if item["is_auto"])
        manual_count = total_count - auto_count
        auto_rate = auto_count / total_count if total_count > 0 else 0.0
        manual_rate = manual_count / total_count if total_count > 0 else 0.0
        return {
            "items": items,
            "auto_rate": round(auto_rate, 4),
            "manual_rate": round(manual_rate, 4),
            "total_count": total_count,
        }

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
        normalized_date_expr = f"REPLACE(CAST({date_column} AS TEXT), '-', '')"
        date_as_date_expr = f"TO_DATE({normalized_date_expr}, 'YYYYMMDD')"
        quantity_expr = f"COALESCE(NULLIF(TRIM(CAST({quantity_column} AS TEXT)), '')::numeric, 0)"
        store_filter_sql = ""
        filter_params: dict[str, object] = {}
        if store_id and store_column:
            store_filter_sql = f" AND CAST({store_column} AS TEXT) = :store_id"
            filter_params["store_id"] = store_id
        recent_window_filter_sql = f"""
                          AND {date_as_date_expr} >= (
                              SELECT MAX(TO_DATE(REPLACE(CAST(window_source.{date_column} AS TEXT), '-', ''), 'YYYYMMDD')) - INTERVAL '27 day'
                              FROM {relation} window_source
                              WHERE NULLIF(TRIM(CAST(window_source.{date_column} AS TEXT)), '') IS NOT NULL
                              {f"AND CAST(window_source.{store_column} AS TEXT) = :store_id" if store_id and store_column else ""}
                          )
        """
        production_exclusion_sql = ""

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
                          {recent_window_filter_sql}
                          {production_exclusion_sql}
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
                              {recent_window_filter_sql}
                              {production_exclusion_sql}
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
                        date_from=date_from or ((datetime.now().date() - timedelta(days=6)).isoformat()),
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

    def get_history(self, store_id: str | None = None, limit: int = 30) -> dict:
        if not self.engine or store_id is None:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}

        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                        SELECT item_nm, dlv_dt, ord_qty, confrm_qty, auto_ord_yn, ord_grp_nm
                        FROM raw_order_extract
                        WHERE masked_stor_cd = :store_id
                        ORDER BY dlv_dt DESC
                        LIMIT :limit
                    """
                        ),
                        {"store_id": store_id, "limit": limit},
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}
        return self._build_history_response(list(rows))

    def get_history_filtered(
        self,
        *,
        store_id: str | None = None,
        limit: int = 100,
        date_from: str | None = None,
        date_to: str | None = None,
        item_nm: str | None = None,
        is_auto: bool | None = None,
        reference_datetime: datetime | None = None,
    ) -> dict:
        if not self.engine:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}

        date_from_norm = self._normalize_yyyymmdd(date_from)
        date_to_norm = self._normalize_yyyymmdd(date_to)
        reference_date_norm, include_same_day = self._build_history_visibility_filter(reference_datetime)

        where_clauses: list[str] = []
        params: dict[str, object] = {"limit": limit}
        if store_id:
            where_clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id
        if date_from_norm:
            where_clauses.append("REPLACE(CAST(dlv_dt AS TEXT), '-', '') >= :date_from")
            params["date_from"] = date_from_norm
        if date_to_norm:
            where_clauses.append("REPLACE(CAST(dlv_dt AS TEXT), '-', '') <= :date_to")
            params["date_to"] = date_to_norm
        if reference_date_norm:
            where_clauses.append(
                f"REPLACE(CAST(dlv_dt AS TEXT), '-', '') {'<=' if include_same_day else '<'} :reference_date"
            )
            params["reference_date"] = reference_date_norm
        if item_nm:
            where_clauses.append("CAST(item_nm AS TEXT) ILIKE :item_nm")
            params["item_nm"] = f"%{item_nm.strip()}%"
        if is_auto is not None:
            where_clauses.append("CAST(auto_ord_yn AS TEXT) = :auto_ord_yn")
            params["auto_ord_yn"] = "1" if is_auto else "0"

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            f"""
                        SELECT
                            CAST(item_nm AS TEXT) AS item_nm,
                            CAST(dlv_dt AS TEXT) AS dlv_dt,
                            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(ord_qty AS TEXT)), '')::numeric, 0))) AS ord_qty,
                            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(confrm_qty AS TEXT)), '')::numeric, 0))) AS confrm_qty,
                            MAX(CAST(auto_ord_yn AS TEXT)) AS auto_ord_yn,
                            MAX(CAST(ord_grp_nm AS TEXT)) AS ord_grp_nm
                        FROM raw_order_extract
                        WHERE {where_sql}
                        GROUP BY CAST(dlv_dt AS TEXT), CAST(item_nm AS TEXT)
                        ORDER BY REPLACE(CAST(dlv_dt AS TEXT), '-', '') DESC, CAST(item_nm AS TEXT)
                        LIMIT :limit
                    """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}
        return self._build_history_response(list(rows))
