from __future__ import annotations
import logging
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config.store_mart_mapping import get_store_mart_table
from app.infrastructure.db.utils import has_table

logger = logging.getLogger(__name__)

_OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_RECOMMENDED_OPTION_ID = f"opt-{chr(97)}"  # 첫 번째 옵션(date_idx=0) canonical ID
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
_STORE_WEATHER_START_DT = "20250311"
_STORE_WEATHER_END_DT = "20260310"


class OrderingRepository:
    _DEFAULT_HISTORY_ORDER_HOUR = 12
    _TREND_FACTOR_MIN = 0.85
    _TREND_FACTOR_MAX = 1.25
    _FINAL_FACTOR_MIN = 0.70
    _FINAL_FACTOR_MAX = 1.35
    _NO_BASE_TREND_FACTOR_MIN = 0.85
    _NO_BASE_TREND_FACTOR_MAX = 1.15
    _DEFAULT_DEADLINE_TIME = "12:00"
    _POC_010_ORDERING_JOIN_TABLE = "mart_ordering_join_poc_010"

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

    def _resolve_ordering_relation(self, store_id: str | None) -> str:
        if (
            store_id == "POC_010"
            and self.engine is not None
            and has_table(self.engine, self._POC_010_ORDERING_JOIN_TABLE)
        ):
            return self._POC_010_ORDERING_JOIN_TABLE
        return "raw_order_extract"

    def uses_ordering_join_table(self, store_id: str | None) -> bool:
        return self._resolve_ordering_relation(store_id) == self._POC_010_ORDERING_JOIN_TABLE

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
    def _normalize_weather_reference_date(reference_date: str | None) -> str | None:
        normalized = OrderingRepository._normalize_yyyymmdd(reference_date)
        if not normalized:
            return None
        if normalized > _STORE_WEATHER_END_DT:
            return _STORE_WEATHER_END_DT
        if normalized < _STORE_WEATHER_START_DT:
            return _STORE_WEATHER_START_DT
        return normalized

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
        normalized_date = self._normalize_weather_reference_date(reference_date)
        if not self.engine or not normalized_date:
            return None
        sido = self._resolve_store_sido(store_id=store_id)

        if not has_table(self.engine, "raw_weather_daily"):
            relation = self._resolve_ordering_relation(store_id)
            if relation == self._POC_010_ORDERING_JOIN_TABLE and has_table(self.engine, relation):
                try:
                    with self.engine.connect() as connection:
                        row = (
                            connection.execute(
                                text(
                                    f"""
                                    SELECT weather_date, weather_region, weather_type,
                                           weather_max_temperature_c, weather_min_temperature_c,
                                           weather_precipitation_probability
                                    FROM {relation}
                                    WHERE store_id = :store_id
                                      AND REPLACE(CAST(weather_date AS TEXT), '-', '') = :reference_date
                                    LIMIT 1
                                    """
                                ),
                                {"store_id": store_id, "reference_date": normalized_date},
                            )
                            .mappings()
                            .first()
                        )
                except SQLAlchemyError:
                    row = None
                if row:
                    return {
                        "region": str(row.get("weather_region") or sido),
                        "forecast_date": self._format_basis_date(
                            row.get("weather_date") or normalized_date
                        ),
                        "weather_type": str(row.get("weather_type") or "날씨"),
                        "max_temperature_c": self._safe_int(
                            row.get("weather_max_temperature_c")
                        ),
                        "min_temperature_c": self._safe_int(
                            row.get("weather_min_temperature_c")
                        ),
                        "precipitation_probability": self._safe_int(
                            row.get("weather_precipitation_probability")
                        ),
                    }
            return None
        weather_columns = self._table_columns("raw_weather_daily")
        max_temp_column = self._pick_column(weather_columns, ("max_temp_c",))
        min_temp_column = self._pick_column(weather_columns, ("min_temp_c",))
        precipitation_probability_column = self._pick_column(
            weather_columns, ("precipitation_probability_max",)
        )

        try:
            with self.engine.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            f"""
                            SELECT
                                weather_dt,
                                sido,
                                avg_temp_c,
                                precipitation_mm,
                                {max_temp_column or 'NULL'} AS max_temp_c,
                                {min_temp_column or 'NULL'} AS min_temp_c,
                                {precipitation_probability_column or 'NULL'} AS precipitation_probability_max
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
            "max_temperature_c": self._safe_int(row.get("max_temp_c"))
            if row.get("max_temp_c") is not None
            else int(round(avg_temp_c)),
            "min_temperature_c": self._safe_int(row.get("min_temp_c"))
            if row.get("min_temp_c") is not None
            else int(round(avg_temp_c)),
            "precipitation_probability": self._safe_int(
                row.get("precipitation_probability_max")
            )
            if row.get("precipitation_probability_max") is not None
            else int(round(precipitation_mm)),
        }

    def _get_weather_from_store_weather_mart(
        self,
        *,
        store_id: str | None,
        reference_date: str | None,
    ) -> dict[str, object] | None:
        normalized_date = self._normalize_weather_reference_date(reference_date)
        if not self.engine or not normalized_date:
            return None

        weather_table = get_store_mart_table(store_id, "ordering", "weather_table") or "mart_store_weather_daily"
        if not has_table(self.engine, weather_table):
            return None

        try:
            with self.engine.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            f"""
                            SELECT
                                weather_dt,
                                sido,
                                weather_type,
                                avg_temp_c,
                                max_temp_c,
                                min_temp_c,
                                precipitation_mm,
                                precipitation_probability_max
                            FROM {weather_table}
                            WHERE weather_dt = :reference_date
                              AND store_id = :store_id
                            LIMIT 1
                            """
                        ),
                        {"reference_date": normalized_date, "store_id": store_id},
                    )
                    .mappings()
                    .first()
                )
        except SQLAlchemyError:
            return None

        if not row:
            return None

        avg_temp_c = float(row.get("avg_temp_c") or 0.0)
        precipitation_mm = float(row.get("precipitation_mm") or 0.0)
        return {
            "region": str(row.get("sido") or ""),
            "forecast_date": f"{normalized_date[:4]}-{normalized_date[4:6]}-{normalized_date[6:8]}",
            "weather_type": str(row.get("weather_type") or self._classify_weather_type(avg_temp_c, precipitation_mm)),
            "max_temperature_c": self._safe_int(row.get("max_temp_c"))
            if row.get("max_temp_c") is not None
            else int(round(avg_temp_c)),
            "min_temperature_c": self._safe_int(row.get("min_temp_c"))
            if row.get("min_temp_c") is not None
            else int(round(avg_temp_c)),
            "precipitation_probability": self._safe_int(row.get("precipitation_probability_max"))
            if row.get("precipitation_probability_max") is not None
            else int(round(precipitation_mm)),
        }

    async def get_weather_forecast(
        self,
        store_id: str | None = None,
        reference_date: str | None = None,
    ) -> dict[str, object] | None:
        weather_from_mart = self._get_weather_from_store_weather_mart(
            store_id=store_id,
            reference_date=reference_date,
        )
        if weather_from_mart is not None:
            return weather_from_mart
        return self._get_weather_for_reference_date(
            store_id=store_id,
            reference_date=reference_date,
        )

    def get_order_arrival_schedule(self, store_id: str | None = None) -> dict[str, str] | None:
        if not self.engine:
            return None

        try:
            with self.engine.connect() as connection:
                if has_table(self.engine, "raw_order_arrival_schedule"):
                    if store_id:
                        row = (
                            connection.execute(
                                text(
                                    """
                                    SELECT
                                        order_deadline_at,
                                        arrival_day_offset,
                                        arrival_expected_at,
                                        arrival_bucket,
                                        applied_reference_note,
                                        COUNT(*) AS hit_count
                                    FROM raw_order_arrival_schedule
                                    WHERE masked_stor_cd = :store_id
                                      AND NULLIF(TRIM(CAST(order_deadline_at AS TEXT)), '') IS NOT NULL
                                    GROUP BY
                                        order_deadline_at,
                                        arrival_day_offset,
                                        arrival_expected_at,
                                        arrival_bucket,
                                        applied_reference_note
                                    ORDER BY hit_count DESC
                                    LIMIT 1
                                    """
                                ),
                                {"store_id": store_id},
                            )
                            .mappings()
                            .first()
                        )
                        if row:
                            return {
                                "order_deadline_at": str(row["order_deadline_at"]),
                                "arrival_day_offset": str(row["arrival_day_offset"] or ""),
                                "arrival_expected_at": str(row["arrival_expected_at"] or ""),
                                "arrival_bucket": str(row["arrival_bucket"] or ""),
                                "reference_note": str(row["applied_reference_note"] or ""),
                            }

                    row = (
                        connection.execute(
                            text(
                                """
                                SELECT
                                    order_deadline_at,
                                    arrival_day_offset,
                                    arrival_expected_at,
                                    arrival_bucket,
                                    applied_reference_note,
                                    COUNT(*) AS hit_count
                                FROM raw_order_arrival_schedule
                                WHERE NULLIF(TRIM(CAST(order_deadline_at AS TEXT)), '') IS NOT NULL
                                GROUP BY
                                    order_deadline_at,
                                    arrival_day_offset,
                                    arrival_expected_at,
                                    arrival_bucket,
                                    applied_reference_note
                                ORDER BY hit_count DESC
                                LIMIT 1
                                """
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if row:
                        return {
                            "order_deadline_at": str(row["order_deadline_at"]),
                            "arrival_day_offset": str(row["arrival_day_offset"] or ""),
                            "arrival_expected_at": str(row["arrival_expected_at"] or ""),
                            "arrival_bucket": str(row["arrival_bucket"] or ""),
                            "reference_note": str(row["applied_reference_note"] or ""),
                        }

                if has_table(self.engine, "raw_order_arrival_reference"):
                    row = (
                        connection.execute(
                            text(
                                """
                                SELECT
                                    order_deadline_at,
                                    arrival_day_offset,
                                    arrival_expected_at,
                                    arrival_bucket,
                                    reference_note_kr
                                FROM raw_order_arrival_reference
                                WHERE NULLIF(TRIM(CAST(order_deadline_at AS TEXT)), '') IS NOT NULL
                                ORDER BY arrival_bucket
                                LIMIT 1
                                """
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if row:
                        return {
                            "order_deadline_at": str(row["order_deadline_at"]),
                            "arrival_day_offset": str(row["arrival_day_offset"] or ""),
                            "arrival_expected_at": str(row["arrival_expected_at"] or ""),
                            "arrival_bucket": str(row["arrival_bucket"] or ""),
                            "reference_note": str(row["reference_note_kr"] or ""),
                        }
        except SQLAlchemyError:
            return None
        return None

    def _table_columns(self, table_name: str) -> dict[str, str]:
        if not self.engine:
            return {}
        try:
            return {column["name"].lower(): column["name"] for column in inspect(self.engine).get_columns(table_name)}
        except SQLAlchemyError:
            pass
        except Exception:
            pass
        default_columns_by_table = {
            "raw_order_extract": {
                "masked_stor_cd": "masked_stor_cd",
                "dlv_dt": "dlv_dt",
                "item_nm": "item_nm",
                "item_cd": "item_cd",
                "ord_qty": "ord_qty",
                "confrm_qty": "confrm_qty",
                "ord_rec_qty": "ord_rec_qty",
                "auto_ord_yn": "auto_ord_yn",
                "ord_grp_nm": "ord_grp_nm",
            },
            self._POC_010_ORDERING_JOIN_TABLE: {
                "masked_stor_cd": "masked_stor_cd",
                "dlv_dt": "dlv_dt",
                "item_nm": "item_nm",
                "item_cd": "item_cd",
                "ord_qty": "ord_qty",
                "confrm_qty": "confrm_qty",
                "ord_rec_qty": "ord_rec_qty",
                "auto_ord_yn": "auto_ord_yn",
                "ord_grp_nm": "ord_grp_nm",
            },
        }
        return default_columns_by_table.get(table_name, {})

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
        except SQLAlchemyError:
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

    def get_order_arrival_schedule_map(
        self,
        *,
        store_id: str | None = None,
        item_codes: list[str] | None = None,
        item_names: list[str] | None = None,
    ) -> dict[str, dict[str, str]]:
        if not self.engine or not has_table(self.engine, "raw_order_arrival_schedule"):
            return {}

        normalized_codes = [str(code).strip() for code in (item_codes or []) if str(code).strip()]
        normalized_names = [str(name).strip() for name in (item_names or []) if str(name).strip()]
        if not normalized_codes and not normalized_names:
            return {}

        where_clauses: list[str] = []
        params: dict[str, object] = {}
        if store_id:
            where_clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id
        item_filters: list[str] = []
        if normalized_codes:
            item_filters.append("NULLIF(TRIM(CAST(item_cd AS TEXT)), '') = ANY(:item_codes)")
            params["item_codes"] = normalized_codes
        if normalized_names:
            item_filters.append("NULLIF(TRIM(CAST(item_nm AS TEXT)), '') = ANY(:item_names)")
            params["item_names"] = normalized_names
        if item_filters:
            where_clauses.append(f"({' OR '.join(item_filters)})")

        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            f"""
                            WITH filtered AS (
                                SELECT
                                    NULLIF(TRIM(CAST(item_cd AS TEXT)), '') AS item_cd,
                                    NULLIF(TRIM(CAST(item_nm AS TEXT)), '') AS item_nm,
                                    NULLIF(TRIM(CAST(order_deadline_at AS TEXT)), '') AS order_deadline_at,
                                    NULLIF(TRIM(CAST(arrival_day_offset AS TEXT)), '') AS arrival_day_offset,
                                    NULLIF(TRIM(CAST(arrival_expected_at AS TEXT)), '') AS arrival_expected_at,
                                    NULLIF(TRIM(CAST(arrival_bucket AS TEXT)), '') AS arrival_bucket
                                FROM raw_order_arrival_schedule
                                WHERE {' AND '.join(where_clauses)}
                            ),
                            counted AS (
                                SELECT
                                    item_cd,
                                    item_nm,
                                    order_deadline_at,
                                    arrival_day_offset,
                                    arrival_expected_at,
                                    arrival_bucket,
                                    COUNT(*) AS hit_count
                                FROM filtered
                                GROUP BY
                                    item_cd,
                                    item_nm,
                                    order_deadline_at,
                                    arrival_day_offset,
                                    arrival_expected_at,
                                    arrival_bucket
                            ),
                            ranked AS (
                                SELECT
                                    *,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY COALESCE(item_cd, item_nm)
                                        ORDER BY
                                            hit_count DESC,
                                            order_deadline_at NULLS LAST,
                                            arrival_day_offset NULLS LAST,
                                            arrival_expected_at NULLS LAST,
                                            arrival_bucket NULLS LAST
                                    ) AS rank_order
                                FROM counted
                            )
                            SELECT
                                item_cd,
                                item_nm,
                                order_deadline_at,
                                arrival_day_offset,
                                arrival_expected_at,
                                arrival_bucket
                            FROM ranked
                            WHERE rank_order = 1
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return {}

        schedule_map: dict[str, dict[str, str]] = {}
        for row in rows:
            payload = {
                "order_deadline_at": str(row.get("order_deadline_at") or ""),
                "arrival_day_offset": str(row.get("arrival_day_offset") or ""),
                "arrival_expected_at": str(row.get("arrival_expected_at") or ""),
                "arrival_bucket": str(row.get("arrival_bucket") or ""),
            }
            item_cd = str(row.get("item_cd") or "").strip()
            item_nm = str(row.get("item_nm") or "").strip()
            if item_cd and item_cd not in schedule_map:
                schedule_map[item_cd] = payload
            if item_nm and item_nm not in schedule_map:
                schedule_map[item_nm] = payload
        return schedule_map

    @staticmethod
    def _is_auto_order_flag(value: object | None) -> bool:
        normalized = str(value or "").strip().lower()
        return normalized in {"1", "y", "yes", "true", "t", "auto", "automatic", "자동"}

    def _build_history_response(
        self,
        rows: list[object],
        *,
        total_count: int | None = None,
        page: int = 1,
        page_size: int | None = None,
    ) -> dict:
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
        response_total_count = total_count if total_count is not None else len(items)
        page_size_value = page_size if page_size is not None else max(len(items), 1)
        auto_count = sum(1 for item in items if item["is_auto"])
        manual_count = len(items) - auto_count
        auto_rate = auto_count / len(items) if items else 0.0
        manual_rate = manual_count / len(items) if items else 0.0
        return {
            "items": items,
            "auto_rate": round(auto_rate, 4),
            "manual_rate": round(manual_rate, 4),
            "total_count": int(response_total_count),
            "page": int(page),
            "page_size": int(page_size_value),
            "total_pages": max(1, math.ceil(int(response_total_count) / max(int(page_size_value), 1))),
        }

    @staticmethod
    def _format_basis_date(value: object) -> str:
        basis = str(value).strip()
        if len(basis) == 8 and basis.isdigit():
            return f"{basis[:4]}-{basis[4:6]}-{basis[6:8]}"
        return basis.replace("/", "-")[:10]

    @staticmethod
    def _clamp(value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, value))

    @staticmethod
    def _build_metric_filter_clauses(
        *,
        store_id: str | None,
        store_column: str | None,
        item_codes: list[str],
        item_code_column: str | None,
        item_names: list[str],
        item_name_column: str | None,
        date_column: str,
    ) -> tuple[list[str], dict[str, object]]:
        filter_clauses: list[str] = [f"NULLIF(TRIM(CAST({date_column} AS TEXT)), '') IS NOT NULL"]
        params: dict[str, object] = {}
        if store_id and store_column:
            filter_clauses.append(f"CAST({store_column} AS TEXT) = :store_id")
            params["store_id"] = store_id

        item_filters: list[str] = []
        if item_codes and item_code_column:
            item_filters.append(f"NULLIF(TRIM(CAST({item_code_column} AS TEXT)), '') = ANY(:item_codes)")
            params["item_codes"] = item_codes
        if item_names and item_name_column:
            item_filters.append(f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '') = ANY(:item_names)")
            params["item_names"] = item_names
        if item_filters:
            filter_clauses.append(f"({' OR '.join(item_filters)})")
        return filter_clauses, params

    def _load_recent_sales_features(
        self,
        *,
        store_id: str | None,
        item_codes: list[str],
        item_names: list[str],
    ) -> dict[str, dict[str, float]]:
        if not self.engine or not has_table(self.engine, "raw_daily_store_item"):
            return {}

        columns = self._table_columns("raw_daily_store_item")
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, ("sale_dt", "date", "biz_dt"))
        item_code_column = self._pick_column(columns, ("item_cd", "item_code", "sku_id"))
        item_name_column = self._pick_column(columns, ("item_nm", "item_name", "product_nm"))
        qty_column = self._pick_column(columns, ("sale_qty", "qty", "ord_qty"))
        if not date_column or not qty_column or (not item_code_column and not item_name_column):
            return {}

        filter_clauses, params = self._build_metric_filter_clauses(
            store_id=store_id,
            store_column=store_column,
            item_codes=item_codes,
            item_code_column=item_code_column,
            item_names=item_names,
            item_name_column=item_name_column,
            date_column=date_column,
        )

        normalized_date_expr = f"REPLACE(CAST({date_column} AS TEXT), '-', '')"
        date_expr = f"TO_DATE({normalized_date_expr}, 'YYYYMMDD')"
        item_code_expr = (
            f"NULLIF(TRIM(CAST({item_code_column} AS TEXT)), '')" if item_code_column else "NULL"
        )
        item_name_expr = (
            f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')" if item_name_column else "NULL"
        )
        qty_expr = f"COALESCE(NULLIF(TRIM(CAST({qty_column} AS TEXT)), '')::numeric, 0)"

        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                            WITH daily AS (
                                SELECT
                                    {item_code_expr} AS item_code,
                                    {item_name_expr} AS item_name,
                                    {date_expr} AS sale_date,
                                    SUM({qty_expr}) AS sale_qty
                                FROM raw_daily_store_item
                                WHERE {' AND '.join(filter_clauses)}
                                GROUP BY
                                    {item_code_expr},
                                    {item_name_expr},
                                    {date_expr}
                            ),
                            windowed AS (
                                SELECT
                                    item_code,
                                    item_name,
                                    sale_date,
                                    sale_qty,
                                    MAX(sale_date) OVER () AS max_sale_date
                                FROM daily
                            )
                            SELECT
                                item_code,
                                item_name,
                                SUM(CASE WHEN sale_date > max_sale_date - INTERVAL '6 day' THEN sale_qty ELSE 0 END) AS qty_7d,
                                SUM(
                                    CASE
                                        WHEN sale_date <= max_sale_date - INTERVAL '7 day'
                                         AND sale_date > max_sale_date - INTERVAL '28 day'
                                        THEN sale_qty
                                        ELSE 0
                                    END
                                ) AS qty_prev_21d
                            FROM windowed
                            GROUP BY item_code, item_name
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return {}

        feature_map: dict[str, dict[str, float]] = {}
        for row in rows:
            qty_7d = float(row.get("qty_7d") or 0.0)
            qty_prev_21d = float(row.get("qty_prev_21d") or 0.0)
            recent_daily = qty_7d / 7.0
            prev_daily = qty_prev_21d / 21.0
            if prev_daily > 0:
                trend_factor = self._clamp(
                    recent_daily / prev_daily,
                    self._TREND_FACTOR_MIN,
                    self._TREND_FACTOR_MAX,
                )
            else:
                trend_factor = 1.08 if recent_daily > 0 else 1.0
            payload = {
                "recent_daily_sales": recent_daily,
                "prev_daily_sales": prev_daily,
                "trend_factor": trend_factor,
                "qty_7d": qty_7d,
            }
            item_code = str(row.get("item_code") or "").strip()
            item_name = str(row.get("item_name") or "").strip()
            if item_code:
                feature_map[item_code] = payload
            if item_name and item_name not in feature_map:
                feature_map[item_name] = payload
        return feature_map

    def _load_inventory_qty_map(
        self,
        *,
        store_id: str | None,
        item_codes: list[str],
        item_names: list[str],
    ) -> dict[str, int]:
        if not self.engine or not has_table(self.engine, "raw_inventory_extract"):
            return {}

        columns = self._table_columns("raw_inventory_extract")
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, ("stock_dt", "date", "biz_dt"))
        item_code_column = self._pick_column(columns, ("item_cd", "item_code", "sku_id"))
        item_name_column = self._pick_column(columns, ("item_nm", "item_name", "product_nm"))
        qty_column = self._pick_column(columns, ("stock_qty", "qty", "remaining_qty"))
        if not date_column or not qty_column or (not item_code_column and not item_name_column):
            return {}

        filter_clauses, params = self._build_metric_filter_clauses(
            store_id=store_id,
            store_column=store_column,
            item_codes=item_codes,
            item_code_column=item_code_column,
            item_names=item_names,
            item_name_column=item_name_column,
            date_column=date_column,
        )

        item_code_expr = (
            f"NULLIF(TRIM(CAST({item_code_column} AS TEXT)), '')" if item_code_column else "NULL"
        )
        item_name_expr = (
            f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')" if item_name_column else "NULL"
        )
        normalized_date_expr = f"REPLACE(CAST({date_column} AS TEXT), '-', '')"
        qty_expr = f"COALESCE(NULLIF(TRIM(CAST({qty_column} AS TEXT)), '')::numeric, 0)"

        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                            WITH filtered AS (
                                SELECT
                                    {item_code_expr} AS item_code,
                                    {item_name_expr} AS item_name,
                                    {qty_expr} AS stock_qty,
                                    TO_DATE({normalized_date_expr}, 'YYYYMMDD') AS stock_date
                                FROM raw_inventory_extract
                                WHERE {' AND '.join(filter_clauses)}
                            ),
                            ranked AS (
                                SELECT
                                    item_code,
                                    item_name,
                                    stock_qty,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY COALESCE(item_code, item_name)
                                        ORDER BY stock_date DESC
                                    ) AS rn
                                FROM filtered
                            )
                            SELECT item_code, item_name, stock_qty
                            FROM ranked
                            WHERE rn = 1
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return {}

        qty_map: dict[str, int] = {}
        for row in rows:
            qty = self._safe_int(row.get("stock_qty"))
            item_code = str(row.get("item_code") or "").strip()
            item_name = str(row.get("item_name") or "").strip()
            if item_code:
                qty_map[item_code] = qty
            if item_name and item_name not in qty_map:
                qty_map[item_name] = qty
        return qty_map

    def _compute_qty_adjustment(
        self,
        *,
        recent_daily_sales: float,
        trend_factor: float,
        inventory_qty: int,
        shelf_life_days: int | None,
    ) -> dict[str, float]:
        if recent_daily_sales <= 0 and inventory_qty <= 0 and shelf_life_days is None:
            return {
                "inventory_cover_days": 0.0,
                "inventory_factor": 1.0,
                "expiry_factor": 1.0,
                "final_factor": self._clamp(
                    trend_factor,
                    self._NO_BASE_TREND_FACTOR_MIN,
                    self._NO_BASE_TREND_FACTOR_MAX,
                ),
                "expiry_risk": "낮음",
            }
        demand_base = max(recent_daily_sales, 1.0)
        inventory_cover_days = float(inventory_qty) / demand_base

        if inventory_cover_days >= 4.0:
            inventory_factor = 0.82
        elif inventory_cover_days >= 3.0:
            inventory_factor = 0.88
        elif inventory_cover_days >= 2.0:
            inventory_factor = 0.94
        elif inventory_cover_days <= 0.5:
            inventory_factor = 1.22
        elif inventory_cover_days <= 1.0:
            inventory_factor = 1.12
        else:
            inventory_factor = 1.0

        expiry_factor = 1.0
        expiry_risk = "낮음"
        if shelf_life_days is not None:
            if shelf_life_days <= 1:
                expiry_factor = 0.78 if inventory_cover_days > 1.5 else 0.90
                expiry_risk = "높음" if inventory_cover_days > 1.0 else "중간"
            elif shelf_life_days <= 2:
                expiry_factor = 0.88 if inventory_cover_days > 2.0 else 0.96
                expiry_risk = "중간" if inventory_cover_days > 1.0 else "낮음"
            elif shelf_life_days <= 3 and inventory_cover_days > 2.5:
                expiry_factor = 0.93
                expiry_risk = "중간"

        final_factor = self._clamp(
            trend_factor * inventory_factor * expiry_factor,
            self._FINAL_FACTOR_MIN,
            self._FINAL_FACTOR_MAX,
        )
        return {
            "inventory_cover_days": round(inventory_cover_days, 2),
            "inventory_factor": inventory_factor,
            "expiry_factor": expiry_factor,
            "final_factor": final_factor,
            "expiry_risk": expiry_risk,
        }

    def _aggregate_option_rows(self, rows: list[object]) -> dict[str, dict[str, object]]:
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
                    "code": item_code or None,
                    "qty": 0,
                },
            )
            if not bucket.get("code") and item_code:
                bucket["code"] = item_code
            bucket["qty"] = int(bucket["qty"]) + self._safe_int(row["quantity"])
        return aggregated

    def _build_adjusted_option_items(
        self,
        *,
        aggregated: dict[str, dict[str, object]],
        store_id: str | None,
    ) -> tuple[list[dict[str, object]], dict[str, float]]:
        item_codes = [
            str(value.get("code") or "").strip()
            for value in aggregated.values()
            if str(value.get("code") or "").strip()
        ]
        item_names = [
            str(value.get("name") or "").strip()
            for value in aggregated.values()
            if str(value.get("name") or "").strip()
        ]
        sales_feature_map = self._load_recent_sales_features(
            store_id=store_id,
            item_codes=item_codes,
            item_names=item_names,
        )
        inventory_qty_map = self._load_inventory_qty_map(
            store_id=store_id,
            item_codes=item_codes,
            item_names=item_names,
        )
        shelf_life_map = self.get_shelf_life_days_map(
            item_codes=item_codes,
            item_names=item_names,
        )

        sorted_items = sorted(
            aggregated.values(),
            key=lambda bucket: (-int(bucket["qty"]), str(bucket["name"])),
        )
        total_base_qty = sum(int(bucket["qty"]) for bucket in sorted_items)
        total_adjusted_qty = 0
        weighted_trend_factor = 0.0
        weighted_inventory_cover = 0.0
        high_expiry_risk_count = 0
        recent_7d_sales_total = 0.0

        items = [
            {
                "sku_id": str(bucket.get("code") or "").strip() or None,
                "sku_name": str(bucket["name"]),
                "quantity": 0,
                "note": None,
            }
            for bucket in sorted_items
        ]

        for index, bucket in enumerate(sorted_items):
            item_code = str(bucket.get("code") or "").strip()
            item_name = str(bucket.get("name") or "").strip()
            base_qty = int(bucket["qty"])
            sales_features = sales_feature_map.get(item_code) or sales_feature_map.get(item_name) or {}
            recent_daily_sales = float(sales_features.get("recent_daily_sales") or 0.0)
            trend_factor = float(sales_features.get("trend_factor") or 1.0)
            inventory_qty = int(inventory_qty_map.get(item_code) or inventory_qty_map.get(item_name) or 0)
            shelf_life_days = shelf_life_map.get(item_code)
            if shelf_life_days is None:
                shelf_life_days = shelf_life_map.get(item_name)
            adjustment = self._compute_qty_adjustment(
                recent_daily_sales=recent_daily_sales,
                trend_factor=trend_factor,
                inventory_qty=inventory_qty,
                shelf_life_days=shelf_life_days,
            )
            adjusted_qty = int(round(base_qty * adjustment["final_factor"]))
            if adjusted_qty <= 0 and base_qty > 0:
                adjusted_qty = 1

            items[index]["quantity"] = adjusted_qty
            total_adjusted_qty += adjusted_qty
            weighted_trend_factor += trend_factor * base_qty
            weighted_inventory_cover += adjustment["inventory_cover_days"] * base_qty
            recent_7d_sales_total += float(sales_features.get("qty_7d") or 0.0)
            if adjustment["expiry_risk"] == "높음":
                high_expiry_risk_count += 1

        metrics = {
            "total_base_qty": float(total_base_qty),
            "total_adjusted_qty": float(total_adjusted_qty),
            "avg_trend_factor": (weighted_trend_factor / total_base_qty) if total_base_qty > 0 else 1.0,
            "avg_inventory_cover": (weighted_inventory_cover / total_base_qty) if total_base_qty > 0 else 0.0,
            "high_expiry_risk_count": float(high_expiry_risk_count),
            "recent_7d_sales_total": recent_7d_sales_total,
            "adjustment_ratio": (float(total_adjusted_qty) / float(total_base_qty)) if total_base_qty > 0 else 1.0,
            "top_item_name": str(sorted_items[0]["name"]) if sorted_items else "",
        }
        return items, metrics

    @staticmethod
    def _build_recommendation_basis_dates(reference_date: str | None = None) -> list[str]:
        normalized = str(reference_date or "").strip().replace("-", "")
        if len(normalized) == 8 and normalized.isdigit():
            base_date = datetime.strptime(normalized, "%Y%m%d").date()
        else:
            base_date = datetime.now().date()
        return [
            (base_date - timedelta(days=7)).strftime("%Y%m%d"),
            (base_date - timedelta(days=14)).strftime("%Y%m%d"),
            (base_date - timedelta(days=28)).strftime("%Y%m%d"),
        ]

    def get_ordering_trend_summary(
        self,
        *,
        store_id: str,
        reference_date: str | None = None,
    ) -> str | None:
        if not self.engine:
            return None
        relation = self._resolve_ordering_relation(store_id)
        if not has_table(self.engine, relation):
            return None
        columns = self._table_columns(relation)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, ("dlv_dt", "ord_dt", "sale_dt"))
        quantity_column = self._pick_column(columns, ("ord_qty", "confrm_qty", "ord_rec_qty", "quantity"))
        if not store_column or not date_column or not quantity_column:
            return None

        normalized = str(reference_date or "").strip().replace("-", "")
        if len(normalized) != 8 or not normalized.isdigit():
            return None

        base_date = datetime.strptime(normalized, "%Y%m%d").date()
        recent_start = (base_date - timedelta(days=6)).strftime("%Y%m%d")
        previous_start = (base_date - timedelta(days=13)).strftime("%Y%m%d")
        previous_end = (base_date - timedelta(days=7)).strftime("%Y%m%d")

        query = text(
            f"""
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN REPLACE(CAST({date_column} AS TEXT), '-', '') BETWEEN :recent_start AND :reference_date
                            THEN COALESCE(NULLIF(TRIM(CAST({quantity_column} AS TEXT)), '')::numeric, 0)
                            ELSE 0
                        END
                    ),
                    0
                ) AS recent_qty,
                COALESCE(
                    SUM(
                        CASE
                            WHEN REPLACE(CAST({date_column} AS TEXT), '-', '') BETWEEN :previous_start AND :previous_end
                            THEN COALESCE(NULLIF(TRIM(CAST({quantity_column} AS TEXT)), '')::numeric, 0)
                            ELSE 0
                        END
                    ),
                    0
                ) AS previous_qty
            FROM {relation}
            WHERE CAST({store_column} AS TEXT) = :store_id
              AND REPLACE(CAST({date_column} AS TEXT), '-', '') BETWEEN :previous_start AND :reference_date
            """
        )

        try:
            with self.engine.connect() as connection:
                row = connection.execute(
                    query,
                    {
                        "store_id": store_id,
                        "recent_start": recent_start,
                        "reference_date": normalized,
                        "previous_start": previous_start,
                        "previous_end": previous_end,
                    },
                ).mappings().first()
        except SQLAlchemyError:
            return None

        if not row:
            return None

        recent_qty = self._safe_int(row.get("recent_qty"))
        previous_qty = self._safe_int(row.get("previous_qty"))
        if recent_qty == 0 and previous_qty == 0:
            return "최근 7일 주문 데이터가 없습니다."
        if previous_qty <= 0:
            return f"최근 7일 주문량은 {recent_qty}개로, 직전 7일 대비 비교 데이터가 없습니다."

        change_ratio = ((recent_qty - previous_qty) / previous_qty) * 100
        direction = "증가" if change_ratio > 0 else "감소" if change_ratio < 0 else "유지"
        return (
            f"최근 7일 주문량은 {recent_qty}개로, 직전 7일 {previous_qty}개 대비 "
            f"{abs(change_ratio):.1f}% {direction}했습니다."
        )

    def _build_options_from_relation(
        self,
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        quantity_candidates: tuple[str, ...],
        store_id: str | None = None,
        reference_date: str | None = None,
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
        basis_dates = self._build_recommendation_basis_dates(reference_date)

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
                options: list[dict] = []
                for date_idx, date_value in enumerate(basis_dates[:3]):
                    rows = connection.execute(
                        text(
                            f"""
                            SELECT
                                {item_name_expr} AS item_name,
                                {item_code_expr} AS item_code,
                                {quantity_expr} AS quantity
                            FROM {relation}
                            WHERE {normalized_date_expr} = :date_value
                              {store_filter_sql}
                              {production_exclusion_sql}
                            """
                        ),
                        {"date_value": str(date_value), **filter_params},
                    ).mappings().all()
                    aggregated = self._aggregate_option_rows(rows)

                    if not aggregated:
                        continue

                    items, calc_metrics = self._build_adjusted_option_items(
                        aggregated=aggregated,
                        store_id=store_id,
                    )
                    if date_idx == 0 and items:
                        items[0]["note"] = "추천 상위 SKU"
                    if items:
                        raw_notes = notes_map[date_idx] if date_idx < len(notes_map) else None
                        reasoning_metrics: list[dict] = [
                            {"key": "기준일", "value": self._format_basis_date(date_value)},
                            {"key": "보정 전 주문량", "value": f"{int(round(calc_metrics['total_base_qty']))}개"},
                            {"key": "추천 주문량", "value": f"{int(round(calc_metrics['total_adjusted_qty']))}개"},
                            {"key": "품목 수", "value": f"{len(aggregated)}개 SKU"},
                            {"key": "최근 7일 판매량", "value": f"{int(round(calc_metrics['recent_7d_sales_total']))}개"},
                            {"key": "판매 추세", "value": f"{calc_metrics['avg_trend_factor']:.2f}x"},
                            {"key": "재고 커버리지", "value": f"{calc_metrics['avg_inventory_cover']:.1f}일"},
                            {"key": "유통기한 고위험", "value": f"{int(round(calc_metrics['high_expiry_risk_count']))}개 SKU"},
                            {"key": "최종 보정계수", "value": f"{calc_metrics['adjustment_ratio']:.2f}x"},
                        ]
                        if calc_metrics["top_item_name"]:
                            reasoning_metrics.append({"key": "주요 품목", "value": str(calc_metrics["top_item_name"])})
                        options.append(
                            {
                                "option_id": f"opt-{chr(97 + date_idx)}",
                                "title": labels[date_idx],
                                "basis": self._format_basis_date(date_value),
                                "description": f"{descriptions[date_idx]} 최근 7일 판매/재고/유통기한을 반영해 수량을 조정했습니다.",
                                "recommended": date_idx == 0,
                                "reasoning_text": raw_notes[0] if raw_notes else "",
                                "reasoning_metrics": reasoning_metrics,
                                "special_factors": [
                                    *(raw_notes[1:] if raw_notes else []),
                                    "최근 7일 판매 추세 반영",
                                    "현재 재고 커버리지 반영",
                                    "유통기한 폐기 리스크 반영",
                                ],
                                "items": items,
                            }
                        )
                return options
        except SQLAlchemyError:
            return []

    def _build_options_from_store_cache(
        self,
        *,
        cache_path: Path,
        store_id: str | None = None,
        reference_date: str | None = None,
    ) -> list[dict]:
        basis_dates = self._build_recommendation_basis_dates(reference_date)
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
            with sqlite3.connect(cache_path) as connection:
                connection.row_factory = sqlite3.Row
                options: list[dict] = []
                for date_idx, date_value in enumerate(basis_dates[:3]):
                    where_clauses = ["REPLACE(CAST(dlv_dt AS TEXT), '-', '') = ?"]
                    params: list[object] = [str(date_value)]
                    if store_id:
                        where_clauses.append("store_id = ?")
                        params.append(store_id)
                    rows = connection.execute(
                        f"""
                        SELECT
                            CAST(item_nm AS TEXT) AS item_name,
                            CAST(item_cd AS TEXT) AS item_code,
                            ROUND(SUM(COALESCE(ord_qty, 0))) AS quantity
                        FROM order_history
                        WHERE {' AND '.join(where_clauses)}
                        GROUP BY CAST(item_nm AS TEXT), CAST(item_cd AS TEXT)
                        """,
                        params,
                    ).fetchall()
                    aggregated = self._aggregate_option_rows(rows)
                    if not aggregated:
                        continue
                    items, calc_metrics = self._build_adjusted_option_items(
                        aggregated=aggregated,
                        store_id=store_id,
                    )
                    if date_idx == 0 and items:
                        items[0]["note"] = "추천 상위 SKU"
                    raw_notes = notes_map[date_idx] if date_idx < len(notes_map) else None
                    reasoning_metrics: list[dict] = [
                        {"key": "기준일", "value": self._format_basis_date(date_value)},
                        {"key": "보정 전 주문량", "value": f"{int(round(calc_metrics['total_base_qty']))}개"},
                        {"key": "추천 주문량", "value": f"{int(round(calc_metrics['total_adjusted_qty']))}개"},
                    ]
                    options.append(
                        {
                            "option_id": f"opt-{chr(97 + date_idx)}",
                            "title": labels[date_idx],
                            "basis": self._format_basis_date(date_value),
                            "description": f"{descriptions[date_idx]} 최근 7일 판매/재고/유통기한을 반영해 수량을 조정했습니다.",
                            "recommended": date_idx == 0,
                            "reasoning_text": raw_notes[0] if raw_notes else "",
                            "reasoning_metrics": reasoning_metrics,
                            "special_factors": [
                                *(raw_notes[1:] if raw_notes else []),
                                "최근 7일 판매 추세 반영",
                                "현재 재고 커버리지 반영",
                                "유통기한 폐기 리스크 반영",
                            ],
                            "items": items,
                        }
                    )
                return options
        except sqlite3.Error:
            return []

    async def list_options(self, store_id: str | None = None, reference_date: str | None = None) -> list[dict]:
        cache_path = self._resolve_store_cache_db_path(store_id)
        if cache_path is not None:
            cache_options = self._build_options_from_store_cache(
                cache_path=cache_path,
                store_id=store_id,
                reference_date=reference_date,
            )
            if cache_options:
                return cache_options

        relation = self._resolve_ordering_relation(store_id)
        if self.engine and has_table(self.engine, relation):
            options = self._build_options_from_relation(
                relation,
                ("dlv_dt", "ord_dt", "sale_dt"),
                ("item_nm", "item_name", "product_nm"),
                ("item_cd", "item_code", "sku_id"),
                ("ord_qty", "confrm_qty", "ord_rec_qty"),
                store_id=store_id,
                reference_date=reference_date,
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
                reference_date=reference_date,
            )
            if options:
                return options
        return []

    async def get_notification_context(self, notification_id: int) -> dict:
        return {
            "notification_id": notification_id,
            "target_path": "/ordering",
            "focus_option_id": _RECOMMENDED_OPTION_ID,
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
            except SQLAlchemyError as exc:
                logger.warning("save_selection 실패: option_id=%s error=%s", payload.get("option_id"), exc)
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
            except SQLAlchemyError as exc:
                logger.warning("list_selection_history 실패: store_id=%s error=%s", store_id, exc)
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
                            if latest and latest["option_id"] == _RECOMMENDED_OPTION_ID
                            else "custom_selected"
                            if latest
                            else "empty"
                        ),
                        "filtered_store_id": store_id,
                        "filtered_date_from": date_from,
                        "filtered_date_to": date_to,
                    }
            except SQLAlchemyError as exc:
                logger.warning("get_selection_summary 실패: store_id=%s error=%s", store_id, exc)
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
        relation = self._resolve_ordering_relation(store_id)
        columns = self._table_columns(relation)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, ("dlv_dt", "ord_dt", "sale_dt"))
        item_name_column = self._pick_column(columns, ("item_nm", "item_name", "product_nm"))
        ord_qty_column = self._pick_column(columns, ("ord_qty",))
        confrm_qty_column = self._pick_column(columns, ("confrm_qty",))
        auto_ord_column = self._pick_column(columns, ("auto_ord_yn",))
        ord_grp_column = self._pick_column(columns, ("ord_grp_nm",))
        if not store_column or not date_column or not item_name_column or not ord_qty_column or not confrm_qty_column:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}

        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            f"""
                        SELECT
                            CAST({item_name_column} AS TEXT) AS item_nm,
                            CAST({date_column} AS TEXT) AS dlv_dt,
                            CAST({ord_qty_column} AS TEXT) AS ord_qty,
                            CAST({confrm_qty_column} AS TEXT) AS confrm_qty,
                            CAST({auto_ord_column or 'NULL'} AS TEXT) AS auto_ord_yn,
                            CAST({ord_grp_column or 'NULL'} AS TEXT) AS ord_grp_nm
                        FROM {relation}
                        WHERE CAST({store_column} AS TEXT) = :store_id
                        ORDER BY REPLACE(CAST({date_column} AS TEXT), '-', '') DESC
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
        page: int = 1,
        date_from: str | None = None,
        date_to: str | None = None,
        item_nm: str | None = None,
        is_auto: bool | None = None,
        reference_datetime: datetime | None = None,
    ) -> dict:
        cache_path = self._resolve_store_cache_db_path(store_id)
        if cache_path is not None:
            return self._get_history_filtered_from_store_cache(
                cache_path=cache_path,
                store_id=store_id,
                limit=limit,
                page=page,
                date_from=date_from,
                date_to=date_to,
                item_nm=item_nm,
                is_auto=is_auto,
                reference_datetime=reference_datetime,
            )

        if not self.engine:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}
        relation = self._resolve_ordering_relation(store_id)
        if not has_table(self.engine, relation):
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}
        columns = self._table_columns(relation)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, ("dlv_dt", "ord_dt", "sale_dt"))
        item_name_column = self._pick_column(columns, ("item_nm", "item_name", "product_nm"))
        ord_qty_column = self._pick_column(columns, ("ord_qty",))
        confrm_qty_column = self._pick_column(columns, ("confrm_qty",))
        auto_ord_column = self._pick_column(columns, ("auto_ord_yn",))
        ord_grp_column = self._pick_column(columns, ("ord_grp_nm",))
        if not date_column or not item_name_column or not ord_qty_column or not confrm_qty_column:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}

        date_from_norm = self._normalize_yyyymmdd(date_from)
        date_to_norm = self._normalize_yyyymmdd(date_to)
        reference_date_norm, include_same_day = self._build_history_visibility_filter(reference_datetime)

        where_clauses: list[str] = []
        page_value = max(int(page), 1)
        limit_value = max(int(limit), 1)
        offset_value = (page_value - 1) * limit_value
        params: dict[str, object] = {"limit": limit_value, "offset": offset_value}
        normalized_date_expr = f"REPLACE(CAST({date_column} AS TEXT), '-', '')"
        if store_id and store_column:
            where_clauses.append(f"CAST({store_column} AS TEXT) = :store_id")
            params["store_id"] = store_id
        if date_from_norm:
            where_clauses.append(f"{normalized_date_expr} >= :date_from")
            params["date_from"] = date_from_norm
        if date_to_norm:
            where_clauses.append(f"{normalized_date_expr} <= :date_to")
            params["date_to"] = date_to_norm
        if reference_date_norm:
            where_clauses.append(
                f"{normalized_date_expr} {'<=' if include_same_day else '<'} :reference_date"
            )
            params["reference_date"] = reference_date_norm
        if item_nm:
            where_clauses.append(f"CAST({item_name_column} AS TEXT) ILIKE :item_nm")
            params["item_nm"] = f"%{item_nm.strip()}%"
        if is_auto is not None and auto_ord_column:
            where_clauses.append(f"CAST({auto_ord_column} AS TEXT) = :auto_ord_yn")
            params["auto_ord_yn"] = "1" if is_auto else "0"

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        try:
            with self.engine.connect() as conn:
                total_count = int(
                    conn.execute(
                        text(
                            f"""
                        WITH grouped AS (
                            SELECT
                                CAST({date_column} AS TEXT) AS dlv_dt,
                                CAST({item_name_column} AS TEXT) AS item_nm
                            FROM {relation}
                            WHERE {where_sql}
                            GROUP BY CAST({date_column} AS TEXT), CAST({item_name_column} AS TEXT)
                        )
                        SELECT COUNT(*) FROM grouped
                    """
                        ),
                        {key: value for key, value in params.items() if key not in {"limit", "offset"}},
                    ).scalar_one()
                )
                rows = (
                    conn.execute(
                        text(
                            f"""
                        SELECT
                            CAST({item_name_column} AS TEXT) AS item_nm,
                            CAST({date_column} AS TEXT) AS dlv_dt,
                            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST({ord_qty_column} AS TEXT)), '')::numeric, 0))) AS ord_qty,
                            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST({confrm_qty_column} AS TEXT)), '')::numeric, 0))) AS confrm_qty,
                            MAX(CAST({auto_ord_column or 'NULL'} AS TEXT)) AS auto_ord_yn,
                            MAX(CAST({ord_grp_column or 'NULL'} AS TEXT)) AS ord_grp_nm
                        FROM {relation}
                        WHERE {where_sql}
                        GROUP BY CAST({date_column} AS TEXT), CAST({item_name_column} AS TEXT)
                        ORDER BY {normalized_date_expr} DESC, CAST({item_name_column} AS TEXT)
                        LIMIT :limit
                        OFFSET :offset
                    """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return {"items": [], "auto_rate": 0.0, "manual_rate": 0.0, "total_count": 0}
        return self._build_history_response(
            list(rows),
            total_count=total_count,
            page=page_value,
            page_size=limit_value,
        )

    def get_deadline_items(
        self,
        *,
        store_id: str,
        reference_datetime: datetime | None = None,
    ) -> list[dict[str, object]]:
        if not self.engine or not store_id:
            return []
        relation = self._resolve_ordering_relation(store_id)
        columns = self._table_columns(relation)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        date_column = self._pick_column(columns, ("dlv_dt", "ord_dt", "sale_dt"))
        item_name_column = self._pick_column(columns, ("item_nm", "item_name", "product_nm"))
        ord_qty_column = self._pick_column(columns, ("ord_qty",))
        if not store_column or not date_column or not item_name_column or not ord_qty_column:
            return []

        reference_date_norm, include_same_day = self._build_history_visibility_filter(reference_datetime)
        if reference_date_norm is None:
            reference_date_norm = datetime.now().strftime("%Y%m%d")
            include_same_day = True
        reference_date = datetime.strptime(reference_date_norm, "%Y%m%d").date()
        visible_reference_date = reference_date if include_same_day else (reference_date - timedelta(days=1))
        visible_reference_date_norm = visible_reference_date.strftime("%Y%m%d")
        window_start = (visible_reference_date - timedelta(days=6)).strftime("%Y%m%d")

        query = text(
            f"""
            WITH recent_orders AS (
                SELECT
                    CAST({item_name_column} AS TEXT) AS item_nm,
                    REPLACE(CAST({date_column} AS TEXT), '-', '') AS dlv_dt_norm,
                    ROUND(SUM(COALESCE(NULLIF(TRIM(CAST({ord_qty_column} AS TEXT)), '')::numeric, 0))) AS ord_qty
                FROM {relation}
                WHERE CAST({store_column} AS TEXT) = :store_id
                  AND REPLACE(CAST({date_column} AS TEXT), '-', '') >= :window_start
                  AND REPLACE(CAST({date_column} AS TEXT), '-', '') <= :visible_reference_date
                GROUP BY CAST({item_name_column} AS TEXT), REPLACE(CAST({date_column} AS TEXT), '-', '')
            )
            SELECT
                item_nm,
                MAX(dlv_dt_norm) AS latest_dlv_dt,
                SUM(ord_qty) AS total_ord_qty,
                MAX(CASE WHEN dlv_dt_norm = :reference_date THEN 1 ELSE 0 END) AS ordered_today
            FROM recent_orders
            GROUP BY item_nm
            ORDER BY MAX(dlv_dt_norm) DESC, SUM(ord_qty) DESC, item_nm
            """
        )

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    query,
                    {
                        "store_id": store_id,
                        "window_start": window_start,
                        "reference_date": reference_date_norm,
                        "visible_reference_date": visible_reference_date_norm,
                    },
                ).mappings().all()
        except SQLAlchemyError:
            return []

        items: list[dict[str, object]] = []
        for row in rows:
            sku_name = str(row.get("item_nm") or "").strip()
            if not sku_name:
                continue
            items.append(
                {
                    "id": "",
                    "sku_name": sku_name,
                    "deadline_at": self._DEFAULT_DEADLINE_TIME,
                    "is_ordered": bool(include_same_day and int(row.get("ordered_today") or 0) > 0),
                    "_latest_dlv_dt": str(row.get("latest_dlv_dt") or ""),
                    "_total_ord_qty": self._safe_int(row.get("total_ord_qty") or 0),
                }
            )
        items.sort(
            key=lambda item: (
                bool(item.get("is_ordered")),
                -self._safe_int(item.get("_latest_dlv_dt")),
                -self._safe_int(item.get("_total_ord_qty")),
                str(item.get("sku_name") or ""),
            )
        )
        for index, item in enumerate(items, start=1):
            item["id"] = f"deadline-{index}"
            item.pop("_latest_dlv_dt", None)
            item.pop("_total_ord_qty", None)
        return items
