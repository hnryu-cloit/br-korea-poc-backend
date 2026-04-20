from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date as date_type
from datetime import datetime, timedelta
from math import asin, cos, radians, sin, sqrt
from urllib.parse import unquote
from xml.etree import ElementTree as ET

import httpx
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.infrastructure.db.utils import has_table

logger = logging.getLogger(__name__)

_SMALLSHOP_RADIUS_URL = "https://apis.data.go.kr/B553077/api/open/sdsc2/storeListInRadius"
_DEFAULT_CENTER = (126.9780, 37.5665)  # 서울시청 (lon, lat)
_GU_CENTER: dict[str, tuple[float, float]] = {
    "강남구": (127.0473, 37.5172),
    "광진구": (127.0824, 37.5385),
    "마포구": (126.9015, 37.5663),
    "송파구": (127.1059, 37.5145),
    "영등포구": (126.8962, 37.5264),
}
_DONG_CENTER: dict[str, tuple[float, float]] = {
    "역삼동": (127.0365, 37.5006),
    "대치동": (127.0568, 37.4945),
    "삼성동": (127.0631, 37.5144),
    "구의동": (127.0911, 37.5447),
    "화양동": (127.0716, 37.5469),
    "자양동": (127.0830, 37.5349),
    "서교동": (126.9200, 37.5552),
    "합정동": (126.9143, 37.5490),
    "연남동": (126.9238, 37.5611),
    "잠실동": (127.0985, 37.5133),
    "문정동": (127.1215, 37.4858),
    "가락동": (127.1188, 37.4958),
    "여의도동": (126.9245, 37.5256),
    "신길동": (126.9188, 37.5067),
    "당산동": (126.9002, 37.5338),
}


class AnalyticsRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def get_metrics(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        if not self.engine:
            return []

        period = self._resolve_period(date_from=date_from, date_to=date_to)

        items: list[dict] = []
        try:
            channel_metrics = self._get_channel_metrics(store_id=store_id, period=period)
        except SQLAlchemyError:
            channel_metrics = None
        if channel_metrics:
            items.extend(
                [
                    channel_metrics["delivery_orders"],
                    channel_metrics["hall_visits"],
                    channel_metrics["app_order_ratio"],
                    channel_metrics["average_ticket"],
                ]
            )

        try:
            sales_metrics = self._get_sales_metrics(store_id=store_id, period=period)
        except SQLAlchemyError:
            sales_metrics = None
        if sales_metrics:
            items.extend([sales_metrics["total_sales"], sales_metrics["coffee_attach_ratio"]])

        try:
            discount_metrics = self._get_discount_metrics(store_id=store_id, period=period)
        except SQLAlchemyError:
            discount_metrics = None
        if discount_metrics:
            items.append(discount_metrics["discount_ratio"])

        return items

    async def get_weather_impact(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        if not self.engine:
            return {
                "date_from": "",
                "date_to": "",
                "items": [],
            }

        required_tables = (
            "raw_weather_daily",
            "raw_store_master",
            "core_channel_sales",
            "core_daily_item_sales",
        )
        if not all(has_table(self.engine, name) for name in required_tables):
            return {
                "date_from": "",
                "date_to": "",
                "items": [],
            }

        resolved_to = (
            self._normalize_date(date_to) if date_to else datetime.now().strftime("%Y%m%d")
        )
        if date_from:
            resolved_from = self._normalize_date(date_from)
        else:
            to_dt = datetime.strptime(resolved_to, "%Y%m%d")
            resolved_from = (to_dt - timedelta(days=364)).strftime("%Y%m%d")

        if resolved_from > resolved_to:
            resolved_from, resolved_to = resolved_to, resolved_from

        params: dict[str, str] = {
            "date_from": resolved_from,
            "date_to": resolved_to,
        }
        store_filter = ""
        if store_id:
            store_filter = "AND c.masked_stor_cd = :store_id"
            params["store_id"] = store_id

        query = text(
            f"""
            WITH daily_channel AS (
                SELECT
                    c.sale_dt,
                    s.sido,
                    SUM(CASE WHEN c.ho_chnl_div LIKE '온라인%' THEN COALESCE(c.ord_cnt, 0) ELSE 0 END) AS delivery_orders,
                    SUM(CASE WHEN c.ho_chnl_div LIKE '오프라인%' THEN COALESCE(c.ord_cnt, 0) ELSE 0 END) AS offline_orders
                FROM core_channel_sales c
                JOIN raw_store_master s ON s.masked_stor_cd = c.masked_stor_cd
                WHERE c.sale_dt BETWEEN :date_from AND :date_to
                {store_filter}
                GROUP BY c.sale_dt, s.sido
            ),
            daily_product AS (
                SELECT
                    i.sale_dt,
                    s.sido,
                    SUM(COALESCE(i.sale_qty, 0)) AS product_qty
                FROM core_daily_item_sales i
                JOIN raw_store_master s ON s.masked_stor_cd = i.masked_stor_cd
                WHERE i.sale_dt BETWEEN :date_from AND :date_to
                {store_filter.replace("c.", "i.")}
                GROUP BY i.sale_dt, s.sido
            ),
            daily_weather AS (
                SELECT
                    w.weather_dt,
                    w.sido,
                    COALESCE(w.avg_temp_c, 0) AS avg_temp_c,
                    COALESCE(w.precipitation_mm, 0) AS precipitation_mm
                FROM raw_weather_daily w
                WHERE w.weather_dt BETWEEN :date_from AND :date_to
            ),
            merged AS (
                SELECT
                    ch.sido,
                    ch.sale_dt,
                    COALESCE(ch.delivery_orders, 0) AS delivery_orders,
                    COALESCE(ch.offline_orders, 0) AS offline_orders,
                    COALESCE(pr.product_qty, 0) AS product_qty,
                    wt.avg_temp_c,
                    wt.precipitation_mm
                FROM daily_channel ch
                LEFT JOIN daily_product pr
                    ON pr.sale_dt = ch.sale_dt AND pr.sido = ch.sido
                JOIN daily_weather wt
                    ON wt.weather_dt = ch.sale_dt AND wt.sido = ch.sido
            )
            SELECT
                sido,
                COUNT(*)::int AS samples,
                ROUND(AVG(avg_temp_c)::numeric, 2) AS avg_temperature,
                ROUND(AVG(precipitation_mm)::numeric, 2) AS avg_precipitation,
                ROUND(COALESCE(CORR(delivery_orders::numeric, avg_temp_c), 0)::numeric, 4) AS delivery_temp_corr,
                ROUND(COALESCE(CORR(delivery_orders::numeric, precipitation_mm), 0)::numeric, 4) AS delivery_rain_corr,
                ROUND(COALESCE(CORR(offline_orders::numeric, avg_temp_c), 0)::numeric, 4) AS offline_temp_corr,
                ROUND(COALESCE(CORR(offline_orders::numeric, precipitation_mm), 0)::numeric, 4) AS offline_rain_corr,
                ROUND(COALESCE(CORR(product_qty::numeric, avg_temp_c), 0)::numeric, 4) AS product_temp_corr,
                ROUND(COALESCE(CORR(product_qty::numeric, precipitation_mm), 0)::numeric, 4) AS product_rain_corr
            FROM merged
            GROUP BY sido
            ORDER BY samples DESC, sido
            """
        )

        try:
            with self.engine.connect() as connection:
                rows = connection.execute(query, params).mappings().all()
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to compute weather impact (store_id=%s, date_from=%s, date_to=%s): %s",
                store_id,
                resolved_from,
                resolved_to,
                exc,
            )
            rows = []

        items = []
        for row in rows:
            items.append(
                {
                    "sido": str(row["sido"] or "기타"),
                    "samples": int(row["samples"] or 0),
                    "avg_temperature": float(row["avg_temperature"] or 0),
                    "avg_precipitation": float(row["avg_precipitation"] or 0),
                    "correlations": [
                        {
                            "metric": "delivery_orders",
                            "temperature_corr": float(row["delivery_temp_corr"] or 0),
                            "precipitation_corr": float(row["delivery_rain_corr"] or 0),
                        },
                        {
                            "metric": "offline_orders",
                            "temperature_corr": float(row["offline_temp_corr"] or 0),
                            "precipitation_corr": float(row["offline_rain_corr"] or 0),
                        },
                        {
                            "metric": "product_qty",
                            "temperature_corr": float(row["product_temp_corr"] or 0),
                            "precipitation_corr": float(row["product_rain_corr"] or 0),
                        },
                    ],
                }
            )

        return {
            "date_from": resolved_from,
            "date_to": resolved_to,
            "items": items,
        }

    @staticmethod
    def _normalize_date(value: str) -> str:
        return value.replace("-", "")

    def _resolve_period(
        self,
        date_from: str | None,
        date_to: str | None,
    ) -> dict[str, str] | None:
        if not date_from and not date_to:
            return None

        normalized_from = self._normalize_date(date_from or date_to or "")
        normalized_to = self._normalize_date(date_to or date_from or "")

        try:
            from_dt = datetime.strptime(normalized_from, "%Y%m%d")
            to_dt = datetime.strptime(normalized_to, "%Y%m%d")
        except ValueError:
            return None

        if from_dt > to_dt:
            from_dt, to_dt = to_dt, from_dt

        day_span = (to_dt - from_dt).days + 1
        prior_to = from_dt - timedelta(days=1)
        prior_from = prior_to - timedelta(days=day_span - 1)

        return {
            "recent_from": from_dt.strftime("%Y%m%d"),
            "recent_to": to_dt.strftime("%Y%m%d"),
            "prior_from": prior_from.strftime("%Y%m%d"),
            "prior_to": prior_to.strftime("%Y%m%d"),
        }

    @staticmethod
    def _build_store_filter(store_id: str | None) -> tuple[str, dict[str, str]]:
        if not store_id:
            return "", {}
        return "WHERE masked_stor_cd = :store_id", {"store_id": store_id}

    def _get_channel_metrics(
        self, store_id: str | None, period: dict[str, str] | None
    ) -> dict | None:
        if not has_table(self.engine, "core_channel_sales"):
            return None

        store_filter, store_params = self._build_store_filter(store_id)

        if period:
            params: dict[str, str] = {**store_params, **period}
            query = text(
                f"""
                WITH daily AS (
                    SELECT
                        sale_dt,
                        SUM(sale_amt) AS total_sales,
                        SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN ord_cnt ELSE 0 END) AS online_orders,
                        SUM(CASE WHEN ho_chnl_div LIKE '오프라인%' THEN ord_cnt ELSE 0 END) AS offline_orders,
                        SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN sale_amt ELSE 0 END) AS online_sales,
                        SUM(ord_cnt) AS total_orders
                    FROM core_channel_sales
                    {store_filter}
                    GROUP BY sale_dt
                )
                SELECT
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN total_sales END), 0) AS recent_total_sales,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN total_sales END), 0) AS prior_total_sales,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN online_orders END), 0) AS recent_online_orders,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN online_orders END), 0) AS prior_online_orders,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN offline_orders END), 0) AS recent_offline_orders,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN offline_orders END), 0) AS prior_offline_orders,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN online_sales END), 0) AS recent_online_sales,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN online_sales END), 0) AS prior_online_sales,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN total_orders END), 0) AS recent_total_orders,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN total_orders END), 0) AS prior_total_orders
                FROM daily
                """
            )
        else:
            params = {**store_params}
            query = text(
                f"""
                WITH daily AS (
                    SELECT
                        sale_dt,
                        SUM(sale_amt) AS total_sales,
                        SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN ord_cnt ELSE 0 END) AS online_orders,
                        SUM(CASE WHEN ho_chnl_div LIKE '오프라인%' THEN ord_cnt ELSE 0 END) AS offline_orders,
                        SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN sale_amt ELSE 0 END) AS online_sales,
                        SUM(ord_cnt) AS total_orders
                    FROM core_channel_sales
                    {store_filter}
                    GROUP BY sale_dt
                ),
                ranked AS (
                    SELECT
                        sale_dt,
                        total_sales,
                        online_orders,
                        offline_orders,
                        online_sales,
                        total_orders,
                        ROW_NUMBER() OVER (ORDER BY sale_dt DESC) AS rn
                    FROM daily
                )
                SELECT
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN total_sales END), 0) AS recent_total_sales,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN total_sales END), 0) AS prior_total_sales,
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN online_orders END), 0) AS recent_online_orders,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN online_orders END), 0) AS prior_online_orders,
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN offline_orders END), 0) AS recent_offline_orders,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN offline_orders END), 0) AS prior_offline_orders,
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN online_sales END), 0) AS recent_online_sales,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN online_sales END), 0) AS prior_online_sales,
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN total_orders END), 0) AS recent_total_orders,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN total_orders END), 0) AS prior_total_orders
                FROM ranked
                """
            )

        with self.engine.connect() as connection:
            summary = connection.execute(query, params).mappings().one()

        recent_total_sales = float(summary["recent_total_sales"] or 0)
        prior_total_sales = float(summary["prior_total_sales"] or 0)
        recent_online_orders = float(summary["recent_online_orders"] or 0)
        prior_online_orders = float(summary["prior_online_orders"] or 0)
        recent_offline_orders = float(summary["recent_offline_orders"] or 0)
        prior_offline_orders = float(summary["prior_offline_orders"] or 0)
        recent_online_sales = float(summary["recent_online_sales"] or 0)
        prior_online_sales = float(summary["prior_online_sales"] or 0)
        recent_total_orders = float(summary["recent_total_orders"] or 0)
        prior_total_orders = float(summary["prior_total_orders"] or 0)

        def fmt_change(recent: float, prior: float, percent_suffix: str = "%") -> tuple[str, str]:
            if prior == 0 and recent == 0:
                return "+0" + percent_suffix, "flat"
            if prior == 0:
                return "+100.0" + percent_suffix, "up"
            change = round(((recent - prior) / prior) * 100, 1)
            trend = "up" if change > 0 else "down" if change < 0 else "flat"
            return f"{change:+.1f}{percent_suffix}", trend

        app_ratio_recent = (
            0
            if recent_total_sales == 0
            else round(recent_online_sales / recent_total_sales * 100, 1)
        )
        app_ratio_prior = (
            0 if prior_total_sales == 0 else round(prior_online_sales / prior_total_sales * 100, 1)
        )
        avg_ticket_recent = (
            0 if recent_total_orders == 0 else round(recent_total_sales / recent_total_orders)
        )
        avg_ticket_prior = (
            0 if prior_total_orders == 0 else round(prior_total_sales / prior_total_orders)
        )

        delivery_change, delivery_trend = fmt_change(recent_online_orders, prior_online_orders)
        hall_change, hall_trend = fmt_change(recent_offline_orders, prior_offline_orders)
        app_ratio_delta = app_ratio_recent - app_ratio_prior
        app_ratio_trend = "up" if app_ratio_delta > 0 else "down" if app_ratio_delta < 0 else "flat"
        avg_ticket_change, avg_ticket_trend = fmt_change(avg_ticket_recent, avg_ticket_prior)

        period_detail = "선택 기간 기준" if period else "최근 7일 기준"
        return {
            "delivery_orders": {
                "label": "배달 건수",
                "value": f"{int(round(recent_online_orders)):,}건",
                "change": delivery_change,
                "trend": delivery_trend,
                "detail": period_detail,
            },
            "hall_visits": {
                "label": "홀 방문 고객",
                "value": f"{int(round(recent_offline_orders)):,}명",
                "change": hall_change,
                "trend": hall_trend,
                "detail": period_detail,
            },
            "app_order_ratio": {
                "label": "앱 주문 비중",
                "value": f"{app_ratio_recent:.1f}%",
                "change": f"{app_ratio_delta:+.1f}%p",
                "trend": app_ratio_trend,
                "detail": "온라인 매출 비중",
            },
            "average_ticket": {
                "label": "평균 객단가",
                "value": f"₩{int(avg_ticket_recent):,}",
                "change": avg_ticket_change,
                "trend": avg_ticket_trend,
                "detail": period_detail,
            },
        }

    def _get_sales_metrics(
        self, store_id: str | None, period: dict[str, str] | None
    ) -> dict | None:
        if not has_table(self.engine, "core_daily_item_sales"):
            return None

        store_filter, store_params = self._build_store_filter(store_id)

        if period:
            params: dict[str, str] = {**store_params, **period}
            query = text(
                f"""
                WITH daily AS (
                    SELECT sale_dt, SUM(net_sale_amt) AS total_sales
                    FROM core_daily_item_sales
                    {store_filter}
                    GROUP BY sale_dt
                ),
                coffee AS (
                    SELECT
                        sale_dt,
                        SUM(CASE WHEN item_nm LIKE '%커피%' OR item_nm LIKE '%아메리카노%' THEN net_sale_amt ELSE 0 END) AS coffee_sales,
                        SUM(net_sale_amt) AS total_sales
                    FROM core_daily_item_sales
                    {store_filter}
                    GROUP BY sale_dt
                )
                SELECT
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN total_sales END), 0) AS recent_total_sales,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN total_sales END), 0) AS prior_total_sales,
                    COALESCE((SELECT SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN coffee_sales END) FROM coffee), 0) AS recent_coffee_sales,
                    COALESCE((SELECT SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN coffee_sales END) FROM coffee), 0) AS prior_coffee_sales,
                    COALESCE((SELECT SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN total_sales END) FROM coffee), 0) AS recent_total_sales_dup,
                    COALESCE((SELECT SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN total_sales END) FROM coffee), 0) AS prior_total_sales_dup
                FROM daily
                """
            )
        else:
            params = {**store_params}
            query = text(
                f"""
                WITH daily AS (
                    SELECT
                        sale_dt,
                        SUM(net_sale_amt) AS total_sales
                    FROM core_daily_item_sales
                    {store_filter}
                    GROUP BY sale_dt
                ),
                ranked AS (
                    SELECT sale_dt, total_sales, ROW_NUMBER() OVER (ORDER BY sale_dt DESC) AS rn
                    FROM daily
                ),
                coffee AS (
                    SELECT
                        SUM(CASE WHEN rn <= 7 THEN coffee_sales END) AS recent_coffee_sales,
                        SUM(CASE WHEN rn > 7 AND rn <= 14 THEN coffee_sales END) AS prior_coffee_sales,
                        SUM(CASE WHEN rn <= 7 THEN total_sales END) AS recent_total_sales,
                        SUM(CASE WHEN rn > 7 AND rn <= 14 THEN total_sales END) AS prior_total_sales
                    FROM (
                        SELECT
                            sale_dt,
                            SUM(CASE WHEN item_nm LIKE '%커피%' OR item_nm LIKE '%아메리카노%' THEN net_sale_amt ELSE 0 END) AS coffee_sales,
                            SUM(net_sale_amt) AS total_sales,
                            ROW_NUMBER() OVER (ORDER BY sale_dt DESC) AS rn
                        FROM core_daily_item_sales
                        {store_filter}
                        GROUP BY sale_dt
                    ) src
                )
                SELECT
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN total_sales END), 0) AS recent_total_sales,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN total_sales END), 0) AS prior_total_sales,
                    COALESCE((SELECT recent_coffee_sales FROM coffee), 0) AS recent_coffee_sales,
                    COALESCE((SELECT prior_coffee_sales FROM coffee), 0) AS prior_coffee_sales,
                    COALESCE((SELECT recent_total_sales FROM coffee), 0) AS recent_total_sales_dup,
                    COALESCE((SELECT prior_total_sales FROM coffee), 0) AS prior_total_sales_dup
                FROM ranked
                """
            )

        with self.engine.connect() as connection:
            summary = connection.execute(query, params).mappings().one()

        recent_total_sales = float(summary["recent_total_sales"] or 0)
        prior_total_sales = float(summary["prior_total_sales"] or 0)
        recent_coffee_sales = float(summary["recent_coffee_sales"] or 0)
        prior_coffee_sales = float(summary["prior_coffee_sales"] or 0)
        coffee_recent_total = float(summary["recent_total_sales_dup"] or 0)
        coffee_prior_total = float(summary["prior_total_sales_dup"] or 0)

        if prior_total_sales == 0 and recent_total_sales == 0:
            sales_change = "+0.0%"
            sales_trend = "flat"
        elif prior_total_sales == 0:
            sales_change = "+100.0%"
            sales_trend = "up"
        else:
            change = round(((recent_total_sales - prior_total_sales) / prior_total_sales) * 100, 1)
            sales_change = f"{change:+.1f}%"
            sales_trend = "up" if change > 0 else "down" if change < 0 else "flat"

        coffee_ratio_recent = (
            0
            if coffee_recent_total == 0
            else round(recent_coffee_sales / coffee_recent_total * 100, 1)
        )
        coffee_ratio_prior = (
            0
            if coffee_prior_total == 0
            else round(prior_coffee_sales / coffee_prior_total * 100, 1)
        )
        coffee_delta = coffee_ratio_recent - coffee_ratio_prior
        coffee_trend = "up" if coffee_delta > 0 else "down" if coffee_delta < 0 else "flat"

        period_detail = "선택 기간 기준" if period else "최근 7일 기준"
        total_sales_label = "선택 기간 총 매출" if period else "이번 주 총 매출"
        return {
            "total_sales": {
                "label": total_sales_label,
                "value": f"₩{int(round(recent_total_sales)):,}",
                "change": sales_change,
                "trend": sales_trend,
                "detail": period_detail,
            },
            "coffee_attach_ratio": {
                "label": "커피 동반 구매율",
                "value": f"{coffee_ratio_recent:.1f}%",
                "change": f"{coffee_delta:+.1f}%p",
                "trend": coffee_trend,
                "detail": "커피 계열 매출 비중",
            },
        }

    def _get_discount_metrics(
        self, store_id: str | None, period: dict[str, str] | None
    ) -> dict | None:
        if not has_table(self.engine, "raw_daily_store_pay_way"):
            return None

        has_settlement = has_table(self.engine, "raw_settlement_master")
        has_policy = has_table(self.engine, "raw_telecom_discount_policy")
        store_filter, store_params = self._build_store_filter(store_id)

        if period:
            params: dict[str, str] = {**store_params, **period}
            query = text(
                f"""
                WITH daily AS (
                    SELECT
                        sale_dt,
                        SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) AS total_amt,
                        SUM(
                            CASE
                                WHEN pay_way_cd IN ('03', '19')
                                THEN COALESCE(NULLIF(pay_amt, '')::numeric, 0)
                                ELSE 0
                            END
                        ) AS discount_amt
                    FROM raw_daily_store_pay_way
                    {store_filter}
                    GROUP BY sale_dt
                )
                SELECT
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN total_amt END), 0) AS recent_total_amt,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN discount_amt END), 0) AS recent_discount_amt,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN total_amt END), 0) AS prior_total_amt,
                    COALESCE(SUM(CASE WHEN sale_dt BETWEEN :prior_from AND :prior_to THEN discount_amt END), 0) AS prior_discount_amt,
                    COALESCE(MAX(CASE WHEN sale_dt BETWEEN :recent_from AND :recent_to THEN sale_dt END), '') AS max_sale_dt
                FROM daily
                """
            )
        else:
            params = {**store_params}
            query = text(
                f"""
                WITH daily AS (
                    SELECT
                        sale_dt,
                        SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) AS total_amt,
                        SUM(
                            CASE
                                WHEN pay_way_cd IN ('03', '19')
                                THEN COALESCE(NULLIF(pay_amt, '')::numeric, 0)
                                ELSE 0
                            END
                        ) AS discount_amt
                    FROM raw_daily_store_pay_way
                    {store_filter}
                    GROUP BY sale_dt
                ),
                ranked AS (
                    SELECT
                        sale_dt,
                        total_amt,
                        discount_amt,
                        ROW_NUMBER() OVER (ORDER BY sale_dt DESC) AS rn
                    FROM daily
                )
                SELECT
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN total_amt END), 0) AS recent_total_amt,
                    COALESCE(SUM(CASE WHEN rn <= 7 THEN discount_amt END), 0) AS recent_discount_amt,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN total_amt END), 0) AS prior_total_amt,
                    COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN discount_amt END), 0) AS prior_discount_amt,
                    COALESCE(MAX(sale_dt), '') AS max_sale_dt
                FROM ranked
                """
            )

        with self.engine.connect() as connection:
            summary = connection.execute(query, params).mappings().one()

            active_settlement_count = 0
            if has_settlement and summary["max_sale_dt"]:
                active_settlement_count = int(
                    connection.execute(
                        text(
                            """
                            SELECT COUNT(*)
                            FROM raw_settlement_master
                            WHERE COALESCE(use_yn, '0') = '1'
                              AND COALESCE(start_dt, '00000000') <= :target_date
                              AND COALESCE(fnsh_dt, '99999999') >= :target_date
                            """
                        ),
                        {"target_date": summary["max_sale_dt"]},
                    ).scalar_one()
                    or 0
                )

            active_policy_count = 0
            if has_policy and summary["max_sale_dt"]:
                active_policy_count = int(
                    connection.execute(
                        text(
                            """
                            SELECT COUNT(*)
                            FROM raw_telecom_discount_policy
                            WHERE COALESCE(use_yn, '0') = '1'
                              AND COALESCE(start_dt, '00000000') <= :target_date
                              AND COALESCE(fnsh_dt, '99999999') >= :target_date
                            """
                        ),
                        {"target_date": summary["max_sale_dt"]},
                    ).scalar_one()
                    or 0
                )

        recent_total_amt = float(summary["recent_total_amt"] or 0)
        recent_discount_amt = float(summary["recent_discount_amt"] or 0)
        prior_total_amt = float(summary["prior_total_amt"] or 0)
        prior_discount_amt = float(summary["prior_discount_amt"] or 0)

        recent_ratio = (
            0 if recent_total_amt == 0 else round(recent_discount_amt / recent_total_amt * 100, 1)
        )
        prior_ratio = (
            0 if prior_total_amt == 0 else round(prior_discount_amt / prior_total_amt * 100, 1)
        )
        ratio_delta = round(recent_ratio - prior_ratio, 1)
        trend = "up" if ratio_delta > 0 else "down" if ratio_delta < 0 else "flat"

        detail_parts = ["제휴할인 + 캠페인할인결제"]
        if period:
            detail_parts.append("선택 기간 기준")
        if active_settlement_count:
            detail_parts.append(f"정산 기준 {active_settlement_count}건")
        if active_policy_count:
            detail_parts.append(f"제휴 정책 {active_policy_count}건")

        return {
            "discount_ratio": {
                "label": "할인 결제 비중",
                "value": f"{recent_ratio:.1f}%",
                "change": f"{ratio_delta:+.1f}%p",
                "trend": trend,
                "detail": " · ".join(detail_parts),
            }
        }

    def get_store_profile(self, store_id: str | None = None) -> dict | None:
        if not self.engine or not has_table(self.engine, "raw_store_master"):
            return None
        try:
            with self.engine.connect() as conn:
                params: dict = {}
                if store_id:
                    params["store_id"] = store_id
                    row = (
                        conn.execute(
                            text(
                                """
                            SELECT masked_stor_cd, maked_stor_nm, sido, region,
                                   store_type, store_area_pyeong, business_type,
                                   COALESCE(NULLIF(TRIM(CAST(actual_sales_amt AS TEXT)), '')::numeric, 0) AS actual_sales_amt
                            FROM raw_store_master
                            WHERE masked_stor_cd = :store_id
                            LIMIT 1
                        """
                            ),
                            params,
                        )
                        .mappings()
                        .first()
                    )
                else:
                    row = (
                        conn.execute(
                            text(
                                """
                            SELECT masked_stor_cd, maked_stor_nm, sido, region,
                                   store_type, store_area_pyeong, business_type,
                                   COALESCE(NULLIF(TRIM(CAST(actual_sales_amt AS TEXT)), '')::numeric, 0) AS actual_sales_amt
                            FROM raw_store_master
                            LIMIT 1
                        """
                            )
                        )
                        .mappings()
                        .first()
                    )

                if row is None:
                    return None

                peer_count = int(
                    conn.execute(
                        text(
                            """
                            SELECT COUNT(*)
                            FROM raw_store_master
                            WHERE sido = :sido AND store_type = :store_type
                        """
                        ),
                        {"sido": row["sido"], "store_type": row["store_type"]},
                    ).scalar_one()
                    or 0
                )
            return {**dict(row), "peer_count": peer_count}
        except SQLAlchemyError as exc:
            logger.warning("get_store_profile 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return None

    def get_customer_profile(self, store_id: str | None = None) -> dict:
        customer_segments: list[dict] = []
        telecom_discounts: list[dict] = []

        if not self.engine:
            return {"customer_segments": customer_segments, "telecom_discounts": telecom_discounts}

        try:
            with self.engine.connect() as conn:
                if has_table(self.engine, "raw_campaign_master"):
                    campaign_columns = {
                        column["name"].lower()
                        for column in inspect(self.engine).get_columns("raw_campaign_master")
                    }
                    campaign_store_col = next(
                        (
                            column
                            for column in ("masked_stor_cd", "store_id", "stor_cd")
                            if column in campaign_columns
                        ),
                        None,
                    )
                    campaign_store_filter = (
                        f" AND {campaign_store_col} = :store_id"
                        if store_id and campaign_store_col
                        else ""
                    )
                    campaign_params: dict[str, str] = {}
                    if store_id and campaign_store_col:
                        campaign_params["store_id"] = store_id
                    rows = (
                        conn.execute(
                            text(
                                f"""
                            SELECT trgt_cust_type_nm, COUNT(*) AS cnt
                            FROM raw_campaign_master
                            WHERE use_yn = '1'
                              {campaign_store_filter}
                            GROUP BY trgt_cust_type_nm
                            ORDER BY cnt DESC
                        """
                            )
                            ,
                            campaign_params,
                        )
                        .mappings()
                        .all()
                    )
                    customer_segments = [
                        {"segment_nm": r["trgt_cust_type_nm"] or "", "count": int(r["cnt"] or 0)}
                        for r in rows
                    ]

                if has_table(self.engine, "raw_telecom_discount_policy"):
                    telecom_columns = {
                        column["name"].lower()
                        for column in inspect(self.engine).get_columns("raw_telecom_discount_policy")
                    }
                    telecom_store_col = next(
                        (
                            column
                            for column in ("masked_stor_cd", "store_id", "stor_cd")
                            if column in telecom_columns
                        ),
                        None,
                    )
                    telecom_store_filter = (
                        f" AND {telecom_store_col} = :store_id"
                        if store_id and telecom_store_col
                        else ""
                    )
                    telecom_params: dict[str, str] = {}
                    if store_id and telecom_store_col:
                        telecom_params["store_id"] = store_id
                    rows = (
                        conn.execute(
                            text(
                                f"""
                            SELECT pay_dc_nm, pay_dc_grp_type_nm, pay_dc_val, pay_dc_methd_nm
                            FROM raw_telecom_discount_policy
                            WHERE use_yn = '1'
                              {telecom_store_filter}
                            ORDER BY grp_prrty
                            LIMIT 6
                        """
                            )
                            ,
                            telecom_params,
                        )
                        .mappings()
                        .all()
                    )
                    telecom_discounts = [
                        {
                            "name": r["pay_dc_nm"] or "",
                            "type_nm": r["pay_dc_grp_type_nm"] or "",
                            "value": str(r["pay_dc_val"] or ""),
                            "method_nm": r["pay_dc_methd_nm"] or "",
                        }
                        for r in rows
                    ]
        except SQLAlchemyError as exc:
            logger.warning("get_customer_profile 쿼리 실패: error=%s", exc)

        return {"customer_segments": customer_segments, "telecom_discounts": telecom_discounts}

    def get_sales_trend(self, store_id: str | None = None) -> dict:
        """이번 달 vs 지난달 누적 매출 + 요일별/시간대별 추이"""
        from calendar import monthrange
        from datetime import date as date_type
        today = datetime.utcnow().date()
        this_month_start = today.replace(day=1)
        last_month_end = this_month_start - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        days_in_this_month = monthrange(today.year, today.month)[1]
        last_month_days = monthrange(last_month_end.year, last_month_end.month)[1]

        dow_labels = ["월", "화", "수", "목", "금", "토", "일"]

        empty = {
            "headline": "데이터를 불러오는 중입니다.",
            "headline_trend": "flat",
            "points": [],
            "insight_chips": [],
            "dow_points": [],
            "hour_points": [],
        }

        if not self.engine:
            return empty

        # 일별 매출 기준 테이블 선택
        daily_table = None
        for t in ("core_daily_item_sales", "raw_daily_store_item"):
            if has_table(self.engine, t):
                daily_table = t
                break
        if not daily_table:
            return empty

        amount_col = "net_sale_amt" if daily_table == "core_daily_item_sales" else "sale_amt"

        def _fmt(d) -> str:
            return d.strftime("%Y%m%d")

        try:
            with self.engine.connect() as conn:
                store_filter = "AND masked_stor_cd = :store_id" if store_id else ""
                params: dict = {"store_id": store_id} if store_id else {}

                # 1. 이번 달 일별 매출
                this_rows = (
                    conn.execute(
                        text(
                            f"""
                        SELECT CAST(sale_dt AS TEXT) AS d,
                               SUM(CAST({amount_col} AS NUMERIC)) AS amt
                        FROM {daily_table}
                        WHERE sale_dt >= :this_start AND sale_dt <= :this_end
                          {store_filter}
                        GROUP BY d ORDER BY d
                    """
                        ),
                        {**params, "this_start": _fmt(this_month_start), "this_end": _fmt(today)},
                    )
                    .mappings()
                    .all()
                )

                # 2. 지난달 일별 매출
                last_rows = (
                    conn.execute(
                        text(
                            f"""
                        SELECT CAST(sale_dt AS TEXT) AS d,
                               SUM(CAST({amount_col} AS NUMERIC)) AS amt
                        FROM {daily_table}
                        WHERE sale_dt >= :last_start AND sale_dt <= :last_end
                          {store_filter}
                        GROUP BY d ORDER BY d
                    """
                        ),
                        {
                            **params,
                            "last_start": _fmt(last_month_start),
                            "last_end": _fmt(last_month_end),
                        },
                    )
                    .mappings()
                    .all()
                )

                # 3. 요일별 평균 매출 (이번달/지난달)
                dow_this: dict[int, list[float]] = {i: [] for i in range(7)}
                dow_last: dict[int, list[float]] = {i: [] for i in range(7)}
                for r in this_rows:
                    d_str = str(r["d"])
                    try:
                        d = date_type(int(d_str[:4]), int(d_str[4:6]), int(d_str[6:8]))
                        dow_this[d.weekday()].append(float(r["amt"] or 0))
                    except (ValueError, IndexError):
                        pass
                for r in last_rows:
                    d_str = str(r["d"])
                    try:
                        d = date_type(int(d_str[:4]), int(d_str[4:6]), int(d_str[6:8]))
                        dow_last[d.weekday()].append(float(r["amt"] or 0))
                    except (ValueError, IndexError):
                        pass

                dow_points = [
                    {
                        "dow": i,
                        "label": dow_labels[i],
                        "this_month_avg": (
                            round(sum(dow_this[i]) / len(dow_this[i]), 0) if dow_this[i] else 0
                        ),
                        "last_month_avg": (
                            round(sum(dow_last[i]) / len(dow_last[i]), 0) if dow_last[i] else 0
                        ),
                    }
                    for i in range(7)
                ]

                # 4. 시간대별 평균 매출 (raw_daily_store_item_tmzon 우선)
                hour_points: list[dict] = []
                if has_table(self.engine, "raw_daily_store_item_tmzon"):
                    # tmzon_div 컬럼에 시간대 코드(0~23) 저장
                    h_this = (
                        conn.execute(
                            text(
                                f"""
                            SELECT CAST(tmzon_div AS INTEGER) AS hr,
                                   AVG(CAST(sale_amt AS NUMERIC)) AS avg_amt
                            FROM raw_daily_store_item_tmzon
                            WHERE sale_dt >= :this_start AND sale_dt <= :this_end
                              {store_filter}
                            GROUP BY hr ORDER BY hr
                        """
                            ),
                            {
                                **params,
                                "this_start": _fmt(this_month_start),
                                "this_end": _fmt(today),
                            },
                        )
                        .mappings()
                        .all()
                    )
                    h_last = (
                        conn.execute(
                            text(
                                f"""
                            SELECT CAST(tmzon_div AS INTEGER) AS hr,
                                   AVG(CAST(sale_amt AS NUMERIC)) AS avg_amt
                            FROM raw_daily_store_item_tmzon
                            WHERE sale_dt >= :last_start AND sale_dt <= :last_end
                              {store_filter}
                            GROUP BY hr ORDER BY hr
                        """
                            ),
                            {
                                **params,
                                "last_start": _fmt(last_month_start),
                                "last_end": _fmt(last_month_end),
                            },
                        )
                        .mappings()
                        .all()
                    )
                    this_by_hour = {int(r["hr"]): float(r["avg_amt"] or 0) for r in h_this}
                    last_by_hour = {int(r["hr"]): float(r["avg_amt"] or 0) for r in h_last}
                    all_hours = sorted(set(this_by_hour) | set(last_by_hour))
                    hour_points = [
                        {
                            "hour": h,
                            "this_month_avg": round(this_by_hour.get(h, 0), 0),
                            "last_month_avg": round(last_by_hour.get(h, 0), 0),
                        }
                        for h in all_hours
                    ]

                # 5. 채널별 인사이트 chip
                channel_chips: list[dict] = []
                if has_table(self.engine, "raw_daily_store_online"):
                    ch_rows = (
                        conn.execute(
                            text(
                                """
                            SELECT online_div_cd,
                                   SUM(CAST(sale_amt AS NUMERIC)) AS amt
                            FROM raw_daily_store_online
                            WHERE sale_dt >= :this_start
                            GROUP BY online_div_cd ORDER BY amt DESC LIMIT 3
                        """
                            ),
                            {"this_start": _fmt(this_month_start)},
                        )
                        .mappings()
                        .all()
                    )
                    for r in ch_rows:
                        channel_chips.append(
                            {
                                "label": r["online_div_cd"] or "기타",
                                "value": f'{int(float(r["amt"] or 0)):,}원',
                                "trend": "up",
                            }
                        )

        except SQLAlchemyError as exc:
            logger.warning("get_sales_trend 쿼리 실패: %s", exc)
            return empty

        # 누적합 계산
        this_daily: dict[int, float] = {}
        for r in this_rows:
            day = int(str(r["d"])[-2:])
            this_daily[day] = float(r["amt"] or 0)

        last_daily: dict[int, float] = {}
        for r in last_rows:
            day = int(str(r["d"])[-2:])
            last_daily[day] = float(r["amt"] or 0)

        max_days = max(days_in_this_month, last_month_days)
        this_cum = 0.0
        last_cum = 0.0
        points = []
        for day in range(1, max_days + 1):
            if day <= days_in_this_month:
                this_cum += this_daily.get(day, 0)
            if day <= last_month_days:
                last_cum += last_daily.get(day, 0)

            projection = None
            if day > today.day and day <= days_in_this_month and today.day > 0:
                daily_avg = this_cum / today.day
                projection = round(this_cum + daily_avg * (day - today.day), 0)

            points.append(
                {
                    "day": day,
                    "this_month": round(this_cum, 0) if day <= today.day else None,
                    "last_month": round(last_cum, 0) if day <= last_month_days else None,
                    "projection": projection,
                }
            )

        # 헤드라인
        this_total = sum(this_daily.get(d, 0) for d in range(1, today.day + 1))
        last_same = sum(last_daily.get(d, 0) for d in range(1, today.day + 1))
        diff = this_total - last_same
        if diff > 0:
            headline = f"지난달 오늘보다 {int(diff):,}원 더 팔았어요."
            headline_trend = "up"
        elif diff < 0:
            headline = f"지난달 오늘보다 {int(abs(diff)):,}원 덜 팔았어요."
            headline_trend = "down"
        else:
            headline = "지난달 오늘과 같은 매출 페이스예요."
            headline_trend = "flat"

        return {
            "headline": headline,
            "headline_trend": headline_trend,
            "points": points,
            "insight_chips": channel_chips,
            "dow_points": dow_points,
            "hour_points": hour_points,
        }

    def get_market_intelligence(
        self,
        store_id: str | None = None,
        gu: str | None = None,
        dong: str | None = None,
        industry: str | None = None,
        year: int | None = None,
        quarter: str | None = None,
        radius_m: int | None = None,
    ) -> dict:
        """상권·경쟁·유동인구 종합 인사이트(POC용 통합 응답)."""
        radius_km = round(max(min((radius_m or 3000) / 1000.0, 3.0), 0.1), 2)
        category_sales_pie: list[dict] = []
        competitors: list[dict] = []
        residential_population_radar: list[dict] = []
        household_composition_pie: list[dict] = []
        estimated_residence_regions: list[dict] = []
        estimated_sales_summary = {
            "monthly_estimated_sales": 0.0,
            "weekly_estimated_sales": 0.0,
            "weekend_ratio": 0.0,
        }
        sales_heatmap: list[dict] = []
        floating_population_trend: list[dict] = []
        analysis = "조회 가능한 실데이터가 없어 상권 분석 지표를 생성하지 못했습니다."
        data_sources: list[str] = []
        industry_analysis = {
            "business_count_trend": [],
            "business_age_5y": [],
        }
        sales_analysis = {
            "monthly_sales_trend": [],
            "monthly_average_sales": 0.0,
        }
        population_analysis = {
            "population_trend": [],
            "income_consumption": [],
        }
        regional_status = {
            "household_count": 0,
            "apartment_household_count": 0,
            "major_facilities_count": 0,
            "transport_access_index": 0.0,
        }
        customer_characteristics = {
            "male_ratio": 0.0,
            "female_ratio": 0.0,
            "new_customer_ratio": None,
            "regular_customer_ratio": None,
            "top_age_group": None,
            "top_visit_time": None,
        }

        sbiz_report_overrides = self._collect_sbiz_live_status_overrides(gu=gu)
        sales_index_summary, sales_index_status = self._fetch_sbiz_sales_index_summary(gu=gu)
        if sales_index_status:
            sbiz_report_overrides["slsIdex"] = sales_index_status

        reference_payload = self._build_reference_market_payload(
            gu=gu,
            dong=dong,
            industry=industry,
            year=year,
            quarter=quarter,
            radius_km=radius_km,
        )
        if reference_payload:
            category_sales_pie = reference_payload.get("category_sales_pie") or category_sales_pie
            competitors = reference_payload.get("competitors") or competitors
            residential_population_radar = (
                reference_payload.get("residential_population_radar") or residential_population_radar
            )
            household_composition_pie = (
                reference_payload.get("household_composition_pie") or household_composition_pie
            )
            estimated_residence_regions = (
                reference_payload.get("estimated_residence_regions") or estimated_residence_regions
            )
            estimated_sales_summary = (
                reference_payload.get("estimated_sales_summary") or estimated_sales_summary
            )
            sales_heatmap = reference_payload.get("sales_heatmap") or sales_heatmap
            floating_population_trend = (
                reference_payload.get("floating_population_trend") or floating_population_trend
            )
            analysis = reference_payload.get("floating_population_analysis") or analysis
            data_sources = reference_payload.get("data_sources") or data_sources
            industry_analysis = reference_payload.get("industry_analysis") or industry_analysis
            sales_analysis = reference_payload.get("sales_analysis") or sales_analysis
            population_analysis = reference_payload.get("population_analysis") or population_analysis
            regional_status = reference_payload.get("regional_status") or regional_status
            customer_characteristics = (
                reference_payload.get("customer_characteristics") or customer_characteristics
            )

        scoped_quarter = self._parse_quarter(quarter)
        new_ratio, regular_ratio = self._get_customer_visit_ratio_from_identified_customers(
            store_id=store_id,
            year=year,
            quarter=scoped_quarter,
        )
        if new_ratio is not None and regular_ratio is not None:
            customer_characteristics["new_customer_ratio"] = new_ratio
            customer_characteristics["regular_customer_ratio"] = regular_ratio
            if "내부 고객식별 컬럼 기반 신규/단골 비율(자동탐지)" not in data_sources:
                data_sources.append("내부 고객식별 컬럼 기반 신규/단골 비율(자동탐지)")

        if (
            customer_characteristics.get("new_customer_ratio") is None
            or customer_characteristics.get("regular_customer_ratio") is None
        ):
            new_ratio, regular_ratio = self._get_customer_visit_ratio_from_cpi(
                store_id=store_id,
                year=year,
                quarter=scoped_quarter,
            )
            if new_ratio is not None and regular_ratio is not None:
                customer_characteristics["new_customer_ratio"] = new_ratio
                customer_characteristics["regular_customer_ratio"] = regular_ratio
                if "내부 고객지표(raw_daily_store_cpi_tmzon) 신규/재방문 비율" not in data_sources:
                    data_sources.append("내부 고객지표(raw_daily_store_cpi_tmzon) 신규/재방문 비율")
            elif "신규/재방문 분류 데이터 미식별(raw_daily_store_cpi_tmzon: 프로모션 중심 cpi)" not in data_sources:
                data_sources.append("신규/재방문 분류 데이터 미식별(raw_daily_store_cpi_tmzon: 프로모션 중심 cpi)")

        if sales_index_summary:
            estimated_sales_summary = {
                "monthly_estimated_sales": float(
                    sales_index_summary.get("monthly_estimated_sales")
                    or estimated_sales_summary.get("monthly_estimated_sales")
                    or 0
                ),
                "weekly_estimated_sales": float(
                    sales_index_summary.get("weekly_estimated_sales")
                    or estimated_sales_summary.get("weekly_estimated_sales")
                    or 0
                ),
                "weekend_ratio": float(
                    sales_index_summary.get("weekend_ratio")
                    or estimated_sales_summary.get("weekend_ratio")
                    or 0
                ),
            }
            if "소상공인365 점포당 매출액 추이 API (SBIZ_API_SALES_INDEX_KEY)" not in data_sources:
                data_sources.append("소상공인365 점포당 매출액 추이 API (SBIZ_API_SALES_INDEX_KEY)")
        if not data_sources:
            data_sources = ["실데이터 소스 미조회: 필터 조건(연도/분기/지역/업종)을 확인하세요."]

        store_reports = self._build_sbiz_store_reports(
            live_status_overrides=sbiz_report_overrides,
        )

        return {
            "radius_km": radius_km,
            "category_sales_pie": category_sales_pie,
            "competitors": competitors,
            "residential_population_radar": residential_population_radar,
            "household_composition_pie": household_composition_pie,
            "estimated_residence_regions": estimated_residence_regions,
            "estimated_sales_summary": estimated_sales_summary,
            "sales_heatmap": sales_heatmap,
            "store_reports": store_reports,
            "floating_population_trend": floating_population_trend,
            "floating_population_analysis": analysis,
            "data_sources": data_sources,
            "industry_analysis": industry_analysis,
            "sales_analysis": sales_analysis,
            "population_analysis": population_analysis,
            "regional_status": regional_status,
            "customer_characteristics": customer_characteristics,
        }

    @staticmethod
    def _normalize_scope_value(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or normalized == "전체":
            return None
        return normalized

    @staticmethod
    def _parse_quarter(quarter: str | None) -> int | None:
        if quarter is None:
            return None
        normalized = quarter.strip().upper().replace(" ", "")
        if normalized.startswith("Q"):
            normalized = normalized[1:]
        if not normalized.isdigit():
            return None
        value = int(normalized)
        return value if value in (1, 2, 3, 4) else None

    @staticmethod
    def _build_quarter_month_filter(
        *,
        year: int | None,
        quarter: int | None,
        date_column: str = "sale_dt",
    ) -> tuple[str, dict[str, object]]:
        conditions: list[str] = []
        params: dict[str, object] = {}
        if year:
            conditions.append(f"LEFT(COALESCE({date_column}, ''), 4) = :year_txt")
            params["year_txt"] = str(year)
        if quarter:
            quarter_months = {
                1: ("01", "02", "03"),
                2: ("04", "05", "06"),
                3: ("07", "08", "09"),
                4: ("10", "11", "12"),
            }
            months = quarter_months.get(quarter)
            if months:
                conditions.append(
                    f"SUBSTRING(COALESCE({date_column}, ''), 5, 2) IN (:q_month_1, :q_month_2, :q_month_3)"
                )
                params["q_month_1"] = months[0]
                params["q_month_2"] = months[1]
                params["q_month_3"] = months[2]
        if not conditions:
            return "", params
        return " AND " + " AND ".join(conditions), params

    def _find_existing_column(self, table_name: str, candidates: tuple[str, ...]) -> str | None:
        if not self.engine or not has_table(self.engine, table_name):
            return None
        inspector = inspect(self.engine)
        column_names = {column.get("name") for column in inspector.get_columns(table_name)}
        for candidate in candidates:
            if candidate in column_names:
                return candidate
        return None

    def _get_customer_visit_ratio_from_identified_customers(
        self,
        *,
        store_id: str | None,
        year: int | None,
        quarter: int | None,
    ) -> tuple[float | None, float | None]:
        if not self.engine or not store_id or not year or not quarter:
            return None, None

        candidate_tables = (
            "raw_daily_store_item",
            "raw_daily_store_pay_way",
            "raw_order_extract",
        )
        for table_name in candidate_tables:
            if not has_table(self.engine, table_name):
                continue

            store_col = self._find_existing_column(
                table_name,
                ("masked_stor_cd", "MASKED_STOR_CD", "store_id", "STOR_CD"),
            )
            date_col = self._find_existing_column(
                table_name,
                ("sale_dt", "SALE_DT", "dlv_dt", "DLV_DT"),
            )
            customer_col = self._find_existing_column(
                table_name,
                (
                    "customer_id",
                    "CUSTOMER_ID",
                    "member_id",
                    "MEMBER_ID",
                    "cust_id",
                    "CUST_ID",
                    "phone_no",
                    "PHONE_NO",
                    "card_no",
                    "CARD_NO",
                ),
            )
            if not store_col or not date_col or not customer_col:
                continue

            quarter_filter_sql, quarter_params = self._build_quarter_month_filter(
                year=year,
                quarter=quarter,
                date_column=date_col,
            )
            try:
                with self.engine.connect() as conn:
                    row = (
                        conn.execute(
                            text(
                                f"""
                                WITH all_visits AS (
                                    SELECT
                                        TRIM(COALESCE({customer_col}, '')) AS customer_key,
                                        MIN(COALESCE({date_col}, '')) AS first_visit_dt
                                    FROM {table_name}
                                    WHERE {store_col} = :store_id
                                      AND TRIM(COALESCE({customer_col}, '')) <> ''
                                    GROUP BY TRIM(COALESCE({customer_col}, ''))
                                ),
                                scoped_visits AS (
                                    SELECT DISTINCT
                                        TRIM(COALESCE({customer_col}, '')) AS customer_key
                                    FROM {table_name}
                                    WHERE {store_col} = :store_id
                                      AND TRIM(COALESCE({customer_col}, '')) <> ''
                                      {quarter_filter_sql}
                                ),
                                customer_scope AS (
                                    SELECT
                                        s.customer_key,
                                        a.first_visit_dt
                                    FROM scoped_visits s
                                    JOIN all_visits a
                                      ON a.customer_key = s.customer_key
                                )
                                SELECT
                                    SUM(
                                        CASE
                                            WHEN LEFT(COALESCE(first_visit_dt, ''), 4) = :year_txt
                                             AND SUBSTRING(COALESCE(first_visit_dt, ''), 5, 2) IN (:q_month_1, :q_month_2, :q_month_3)
                                            THEN 1 ELSE 0
                                        END
                                    ) AS new_customer_count,
                                    SUM(
                                        CASE
                                            WHEN NOT (
                                                LEFT(COALESCE(first_visit_dt, ''), 4) = :year_txt
                                                AND SUBSTRING(COALESCE(first_visit_dt, ''), 5, 2) IN (:q_month_1, :q_month_2, :q_month_3)
                                            )
                                            THEN 1 ELSE 0
                                        END
                                    ) AS regular_customer_count
                                FROM customer_scope
                                """
                            ),
                            {
                                "store_id": store_id,
                                **quarter_params,
                            },
                        )
                        .mappings()
                        .one()
                    )
                new_count = float(row.get("new_customer_count") or 0)
                regular_count = float(row.get("regular_customer_count") or 0)
                total = new_count + regular_count
                if total > 0:
                    return round(new_count / total * 100, 1), round(regular_count / total * 100, 1)
            except Exception:
                continue

        return None, None

    def _get_customer_visit_ratio_from_cpi(
        self,
        *,
        store_id: str | None,
        year: int | None,
        quarter: int | None,
    ) -> tuple[float | None, float | None]:
        if not self.engine or not store_id:
            return None, None
        if not has_table(self.engine, "raw_daily_store_cpi_tmzon"):
            return None, None

        quarter_filter_sql, quarter_params = self._build_quarter_month_filter(
            year=year,
            quarter=quarter,
        )

        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            f"""
                            SELECT
                                cpi_nm,
                                SUM(COALESCE(NULLIF(bill_cnt, ''), '0')::numeric) AS bill_cnt
                            FROM raw_daily_store_cpi_tmzon
                            WHERE masked_stor_cd = :store_id
                            {quarter_filter_sql}
                            GROUP BY cpi_nm
                            """
                        ),
                        {
                            "store_id": store_id,
                            **quarter_params,
                        },
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            return None, None

        new_keywords = ("신규", "첫", "초방문", "first", "new")
        regular_keywords = ("단골", "재방문", "기존", "충성", "repeat", "return")

        new_count = 0.0
        regular_count = 0.0
        for row in rows:
            cpi_name = str(row.get("cpi_nm") or "").lower()
            bill_count = float(row.get("bill_cnt") or 0)
            if any(keyword in cpi_name for keyword in new_keywords):
                new_count += bill_count
            if any(keyword in cpi_name for keyword in regular_keywords):
                regular_count += bill_count

        total = new_count + regular_count
        if total <= 0:
            return None, None
        return round(new_count / total * 100, 1), round(regular_count / total * 100, 1)

    def _build_reference_market_payload(
        self,
        *,
        gu: str | None,
        dong: str | None,
        industry: str | None,
        year: int | None,
        quarter: str | None,
        radius_km: float,
    ) -> dict | None:
        if not self.engine:
            return None
        if not has_table(self.engine, "raw_seoul_market_sales") or not has_table(
            self.engine, "raw_seoul_market_floating_population"
        ):
            return None

        scope_gu = self._normalize_scope_value(gu)
        scope_dong = self._normalize_scope_value(dong)
        scope_industry = self._normalize_scope_value(industry)
        scope_quarter = self._parse_quarter(quarter)

        sales_filters = ["monthly_sales_amount IS NOT NULL"]
        sales_params: dict[str, object] = {}
        pop_filters = ["total_population IS NOT NULL"]
        pop_params: dict[str, object] = {}

        if year:
            sales_filters.append("base_year = :year")
            pop_filters.append("base_year = :year")
            sales_params["year"] = year
            pop_params["year"] = year
        if scope_quarter:
            sales_filters.append("base_quarter = :quarter")
            pop_filters.append("base_quarter = :quarter")
            sales_params["quarter"] = scope_quarter
            pop_params["quarter"] = scope_quarter
        if scope_gu:
            sales_filters.append("area_name LIKE :gu")
            pop_filters.append("area_name LIKE :gu")
            sales_params["gu"] = f"%{scope_gu}%"
            pop_params["gu"] = f"%{scope_gu}%"
        if scope_dong:
            sales_filters.append("area_name LIKE :dong")
            pop_filters.append("area_name LIKE :dong")
            sales_params["dong"] = f"%{scope_dong}%"
            pop_params["dong"] = f"%{scope_dong}%"
        if scope_industry:
            sales_filters.append("service_name LIKE :industry")
            sales_params["industry"] = f"%{scope_industry}%"

        sales_where = " AND ".join(sales_filters)
        pop_where = " AND ".join(pop_filters)
        fallback_sales_filters = [
            condition
            for condition in sales_filters
            if condition not in {"base_year = :year", "base_quarter = :quarter"}
        ]
        fallback_pop_filters = [
            condition
            for condition in pop_filters
            if condition not in {"base_year = :year", "base_quarter = :quarter"}
        ]
        period_fallback_used = False

        used_smallshop = False
        with self.engine.connect() as conn:
            pie_rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            CASE
                                WHEN service_name LIKE '%제과%' THEN '제과'
                                WHEN service_name LIKE '%커피%' OR service_name LIKE '%카페%' THEN '커피'
                                ELSE '기타'
                            END AS category,
                            SUM(COALESCE(monthly_sales_amount, 0)) AS sales_amount
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        GROUP BY category
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .all()
            )

            category_rows = [row for row in pie_rows if row["category"] in ("제과", "커피")]
            category_total = sum(float(row["sales_amount"] or 0) for row in category_rows)
            if category_total <= 0:
                if year or scope_quarter:
                    fallback_sales_where = " AND ".join(fallback_sales_filters)
                    fallback_sales_params = {
                        key: value
                        for key, value in sales_params.items()
                        if key not in {"year", "quarter"}
                    }
                    fallback_pop_where = " AND ".join(fallback_pop_filters)
                    fallback_pop_params = {
                        key: value
                        for key, value in pop_params.items()
                        if key not in {"year", "quarter"}
                    }
                    pie_rows = (
                        conn.execute(
                            text(
                                f"""
                                SELECT
                                    CASE
                                        WHEN service_name LIKE '%제과%' THEN '제과'
                                        WHEN service_name LIKE '%커피%' OR service_name LIKE '%카페%' THEN '커피'
                                        ELSE '기타'
                                    END AS category,
                                    SUM(COALESCE(monthly_sales_amount, 0)) AS sales_amount
                                FROM raw_seoul_market_sales
                                WHERE {fallback_sales_where}
                                GROUP BY category
                                """
                            ),
                            fallback_sales_params,
                        )
                        .mappings()
                        .all()
                    )
                    category_rows = [row for row in pie_rows if row["category"] in ("제과", "커피")]
                    category_total = sum(float(row["sales_amount"] or 0) for row in category_rows)
                    if category_total <= 0:
                        return None
                    sales_where = fallback_sales_where
                    pop_where = fallback_pop_where
                    sales_params = fallback_sales_params
                    pop_params = fallback_pop_params
                    period_fallback_used = True
                else:
                    return None
            category_sales_pie = [
                {
                    "category": str(row["category"]),
                    "sales_amount": float(row["sales_amount"] or 0),
                    "share_ratio": round(float(row["sales_amount"] or 0) / category_total * 100, 1),
                }
                for row in category_rows
            ]

            industry_trend_rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            base_year,
                            base_quarter,
                            COUNT(DISTINCT (area_name || '|' || service_name)) AS business_count
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        GROUP BY base_year, base_quarter
                        ORDER BY base_year, base_quarter
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .all()
            )
            industry_business_count_trend = [
                {
                    "period": f"{int(row['base_year'])}-Q{int(row['base_quarter'])}",
                    "business_count": int(row["business_count"] or 0),
                }
                for row in industry_trend_rows[-8:]
            ]

            industry_age_rows = (
                conn.execute(
                    text(
                        f"""
                        WITH entity_years AS (
                            SELECT
                                area_name,
                                service_name,
                                COUNT(DISTINCT base_year) AS active_years
                            FROM raw_seoul_market_sales
                            WHERE {sales_where}
                            GROUP BY area_name, service_name
                        )
                        SELECT
                            CASE
                                WHEN active_years >= 5 THEN '5년 이상'
                                WHEN active_years = 4 THEN '4년'
                                WHEN active_years = 3 THEN '3년'
                                WHEN active_years = 2 THEN '2년'
                                ELSE '1년 이하'
                            END AS bucket,
                            COUNT(*) AS business_count
                        FROM entity_years
                        GROUP BY bucket
                        ORDER BY
                            CASE bucket
                                WHEN '1년 이하' THEN 1
                                WHEN '2년' THEN 2
                                WHEN '3년' THEN 3
                                WHEN '4년' THEN 4
                                ELSE 5
                            END
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .all()
            )
            industry_business_age_5y = [
                {
                    "bucket": str(row["bucket"] or ""),
                    "business_count": int(row["business_count"] or 0),
                }
                for row in industry_age_rows
            ]

            sales_monthly_rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            base_year,
                            base_quarter,
                            SUM(COALESCE(monthly_sales_count, 0)) AS sales_count,
                            SUM(COALESCE(monthly_sales_amount, 0)) AS sales_amount
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        GROUP BY base_year, base_quarter
                        ORDER BY base_year, base_quarter
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .all()
            )
            sales_monthly_trend = [
                {
                    "period": f"{int(row['base_year'])}-Q{int(row['base_quarter'])}",
                    "sales_count": float(row["sales_count"] or 0),
                    "sales_amount": float(row["sales_amount"] or 0),
                }
                for row in sales_monthly_rows[-8:]
            ]
            sales_average_row = (
                conn.execute(
                    text(
                        f"""
                        SELECT AVG(COALESCE(monthly_sales_amount, 0)) AS monthly_average_sales
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .one()
            )
            monthly_average_sales = float(sales_average_row["monthly_average_sales"] or 0)

            competitor_rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            area_name,
                            service_name,
                            SUM(COALESCE(monthly_sales_amount, 0)) AS sales_amount
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        GROUP BY area_name, service_name
                        ORDER BY sales_amount DESC
                        LIMIT 10
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .all()
            )

            competitors: list[dict] = []
            for index, row in enumerate(competitor_rows, start=1):
                area_name = str(row["area_name"] or "")
                service_name = str(row["service_name"] or "")
                trend_rows = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                base_year,
                                base_quarter,
                                SUM(COALESCE(monthly_sales_amount, 0)) AS sales_amount
                            FROM raw_seoul_market_sales
                            WHERE area_name = :area_name
                              AND service_name = :service_name
                            GROUP BY base_year, base_quarter
                            ORDER BY base_year, base_quarter
                            """
                        ),
                        {"area_name": area_name, "service_name": service_name},
                    )
                    .mappings()
                    .all()
                )
                trend_rows = trend_rows[-6:]
                sales_trend = [
                    {
                        "month": f"{int(point['base_year'])}-Q{int(point['base_quarter'])}",
                        "sales_amount": float(point["sales_amount"] or 0),
                    }
                    for point in trend_rows
                ]
                if not sales_trend:
                    continue
                first_sales = float(sales_trend[0]["sales_amount"])
                last_sales = float(sales_trend[-1]["sales_amount"])
                trend_direction = (
                    "up" if last_sales > first_sales else "down" if last_sales < first_sales else "flat"
                )

                demographic_row = (
                    conn.execute(
                        text(
                            """
                            SELECT
                                SUM(COALESCE(male_sales_amount, 0)) AS male_sales_amount,
                                SUM(COALESCE(female_sales_amount, 0)) AS female_sales_amount,
                                SUM(COALESCE(age10_sales_amount, 0)) AS age10_sales_amount,
                                SUM(COALESCE(age20_sales_amount, 0)) AS age20_sales_amount,
                                SUM(COALESCE(age30_sales_amount, 0)) AS age30_sales_amount,
                                SUM(COALESCE(age40_sales_amount, 0) + COALESCE(age50_sales_amount, 0) + COALESCE(age60_plus_sales_amount, 0)) AS age40_plus_sales_amount
                            FROM raw_seoul_market_sales
                            WHERE area_name = :area_name
                              AND service_name = :service_name
                            """
                        ),
                        {"area_name": area_name, "service_name": service_name},
                    )
                    .mappings()
                    .one()
                )
                male_amount = float(demographic_row["male_sales_amount"] or 0)
                female_amount = float(demographic_row["female_sales_amount"] or 0)
                total_gender = max(male_amount + female_amount, 1.0)
                male_ratio = male_amount / total_gender
                age_map = {
                    "10대": float(demographic_row["age10_sales_amount"] or 0),
                    "20대": float(demographic_row["age20_sales_amount"] or 0),
                    "30대": float(demographic_row["age30_sales_amount"] or 0),
                    "40대+": float(demographic_row["age40_plus_sales_amount"] or 0),
                }
                age_total = max(sum(age_map.values()), 1.0)
                payment_demographics: list[dict] = []
                for label, amount in age_map.items():
                    payment_base = max(int(round(amount / age_total * 1000)), 1)
                    male_count = int(round(payment_base * male_ratio))
                    female_count = max(payment_base - male_count, 0)
                    payment_demographics.append(
                        {
                            "age_group": label,
                            "male_payment_count": male_count,
                            "female_payment_count": female_count,
                        }
                    )

                competitors.append(
                    {
                        "rank": index,
                        "brand_name": service_name,
                        "store_name": area_name,
                        "distance_km": round(min(radius_km, 3.0) * (index / 10.0), 2),
                        "trend_direction": trend_direction,
                        "sales_trend": sales_trend,
                        "payment_demographics": payment_demographics,
                    }
                )

            live_competitors, smallshop_key_source = self._fetch_smallshop_competitors(
                gu=scope_gu,
                dong=scope_dong,
                industry=scope_industry,
                radius_km=radius_km,
            )
            if not live_competitors:
                live_competitors, smallshop_key_source = self._fetch_sbiz_store_status_competitors(
                    gu=scope_gu,
                    dong=scope_dong,
                    radius_km=radius_km,
                )
            used_smallshop = bool(live_competitors)
            if live_competitors:
                competitors = self._merge_competitor_trends_from_reference(
                    live_competitors=live_competitors,
                    fallback_competitors=competitors,
                )

            pop_row = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            SUM(COALESCE(male_population, 0)) AS male_population,
                            SUM(COALESCE(female_population, 0)) AS female_population,
                            SUM(COALESCE(age10_population, 0)) AS age10_population,
                            SUM(COALESCE(age20_population, 0)) AS age20_population,
                            SUM(COALESCE(age30_population, 0)) AS age30_population,
                            SUM(COALESCE(age40_population, 0)) AS age40_population,
                            SUM(COALESCE(age50_population, 0)) AS age50_population,
                            SUM(COALESCE(age60_plus_population, 0)) AS age60_plus_population,
                            SUM(COALESCE(time_slot1_population, 0)) AS time_slot1_population,
                            SUM(COALESCE(time_slot2_population, 0)) AS time_slot2_population,
                            SUM(COALESCE(time_slot3_population, 0)) AS time_slot3_population,
                            SUM(COALESCE(time_slot4_population, 0)) AS time_slot4_population,
                            SUM(COALESCE(time_slot5_population, 0)) AS time_slot5_population,
                            SUM(COALESCE(time_slot6_population, 0)) AS time_slot6_population
                        FROM raw_seoul_market_floating_population
                        WHERE {pop_where}
                        """
                    ),
                    pop_params,
                )
                .mappings()
                .one()
            )
            male_total = float(pop_row["male_population"] or 0)
            female_total = float(pop_row["female_population"] or 0)
            gender_total = max(male_total + female_total, 1.0)
            male_ratio = male_total / gender_total
            age_values = [
                ("19세 이하", float(pop_row["age10_population"] or 0)),
                ("20대", float(pop_row["age20_population"] or 0)),
                ("30대", float(pop_row["age30_population"] or 0)),
                ("40대", float(pop_row["age40_population"] or 0)),
                ("50대", float(pop_row["age50_population"] or 0)),
                ("60대 이상", float(pop_row["age60_plus_population"] or 0)),
            ]
            residential_population_radar = [
                {
                    "age_group": label,
                    "male_population": int(round(value * male_ratio)),
                    "female_population": max(int(round(value - (value * male_ratio))), 0),
                }
                for label, value in age_values
            ]
            age10_population = float(pop_row["age10_population"] or 0)
            age20_population = float(pop_row["age20_population"] or 0)
            age30_population = float(pop_row["age30_population"] or 0)
            age40_population = float(pop_row["age40_population"] or 0)
            age50_population = float(pop_row["age50_population"] or 0)
            age60_plus_population = float(pop_row["age60_plus_population"] or 0)

            one_person_proxy = age20_population + age30_population + (age40_population * 0.3)
            three_person_proxy = (
                age10_population
                + age50_population
                + age60_plus_population
                + (age40_population * 0.7)
            )
            household_proxy_total = max(one_person_proxy + three_person_proxy, 1.0)
            household_composition_pie = [
                {
                    "household_type": "1인가구",
                    "household_count": int(round(one_person_proxy)),
                    "share_ratio": round(one_person_proxy / household_proxy_total * 100, 1),
                },
                {
                    "household_type": "3인가족",
                    "household_count": int(round(three_person_proxy)),
                    "share_ratio": round(three_person_proxy / household_proxy_total * 100, 1),
                },
            ]

            residence_filters = ["total_population IS NOT NULL"]
            residence_params: dict[str, object] = {}
            if year and not period_fallback_used:
                residence_filters.append("base_year = :year")
                residence_params["year"] = year
            if scope_quarter and not period_fallback_used:
                residence_filters.append("base_quarter = :quarter")
                residence_params["quarter"] = scope_quarter
            if scope_gu:
                residence_filters.append("area_name LIKE :gu")
                residence_params["gu"] = f"%{scope_gu}%"
            elif scope_dong:
                residence_filters.append("area_name LIKE :dong")
                residence_params["dong"] = f"%{scope_dong}%"
            residence_where = " AND ".join(residence_filters)
            residence_rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            area_name,
                            SUM(COALESCE(total_population, 0)) AS total_population
                        FROM raw_seoul_market_floating_population
                        WHERE {residence_where}
                        GROUP BY area_name
                        ORDER BY total_population DESC
                        LIMIT 5
                        """
                    ),
                    residence_params,
                )
                .mappings()
                .all()
            )
            residence_total = sum(float(row["total_population"] or 0) for row in residence_rows)
            estimated_residence_regions = [
                {
                    "region_name": str(row["area_name"] or ""),
                    "share_ratio": round(
                        (float(row["total_population"] or 0) / max(residence_total, 1.0)) * 100,
                        1,
                    ),
                    "estimated_customers": int(round(float(row["total_population"] or 0))),
                }
                for row in residence_rows
            ]

            pop_trend_rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            base_year,
                            base_quarter,
                            SUM(COALESCE(total_population, 0)) AS floating_population,
                            SUM(COALESCE(time_slot5_population, 0) + COALESCE(time_slot6_population, 0)) AS residential_population,
                            SUM(COALESCE(time_slot2_population, 0) + COALESCE(time_slot3_population, 0) + COALESCE(time_slot4_population, 0)) AS worker_population
                        FROM raw_seoul_market_floating_population
                        WHERE {pop_where}
                        GROUP BY base_year, base_quarter
                        ORDER BY base_year, base_quarter
                        """
                    ),
                    pop_params,
                )
                .mappings()
                .all()
            )
            population_trend = [
                {
                    "period": f"{int(row['base_year'])}-Q{int(row['base_quarter'])}",
                    "floating_population": int(float(row["floating_population"] or 0)),
                    "residential_population": int(float(row["residential_population"] or 0)),
                    "worker_population": int(float(row["worker_population"] or 0)),
                }
                for row in pop_trend_rows[-8:]
            ]

            income_rows = (
                conn.execute(
                    text(
                        f"""
                        WITH base AS (
                            SELECT
                                COALESCE(monthly_sales_amount, 0) AS sales_amount,
                                COALESCE(monthly_sales_count, 0) AS sales_count,
                                CASE
                                    WHEN COALESCE(monthly_sales_count, 0) <= 0 THEN '미분류'
                                    WHEN (COALESCE(monthly_sales_amount, 0) / NULLIF(monthly_sales_count, 0)) < 12000 THEN '저소비'
                                    WHEN (COALESCE(monthly_sales_amount, 0) / NULLIF(monthly_sales_count, 0)) < 22000 THEN '중소비'
                                    ELSE '고소비'
                                END AS segment
                            FROM raw_seoul_market_sales
                            WHERE {sales_where}
                        )
                        SELECT
                            segment,
                            SUM(sales_count) AS estimated_customers,
                            SUM(sales_amount) AS sales_amount
                        FROM base
                        GROUP BY segment
                        ORDER BY
                            CASE segment
                                WHEN '저소비' THEN 1
                                WHEN '중소비' THEN 2
                                WHEN '고소비' THEN 3
                                ELSE 4
                            END
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .all()
            )
            total_income_sales = sum(float(row["sales_amount"] or 0) for row in income_rows) or 1.0
            income_consumption = [
                {
                    "segment": str(row["segment"] or "미분류"),
                    "estimated_customers": int(float(row["estimated_customers"] or 0)),
                    "sales_share_ratio": round(float(row["sales_amount"] or 0) / total_income_sales * 100, 1),
                }
                for row in income_rows
            ]

            time_slot_values = {
                "새벽(00-06)": float(pop_row["time_slot1_population"] or 0),
                "오전(06-11)": float(pop_row["time_slot2_population"] or 0),
                "점심(11-14)": float(pop_row["time_slot3_population"] or 0),
                "오후(14-17)": float(pop_row["time_slot4_population"] or 0),
                "저녁(17-21)": float(pop_row["time_slot5_population"] or 0),
                "밤(21-24)": float(pop_row["time_slot6_population"] or 0),
            }
            top_visit_time = max(time_slot_values.items(), key=lambda item: item[1])[0]
            top_age_group = max(age_values, key=lambda item: item[1])[0] if age_values else None
            floating_avg = (
                sum(item["floating_population"] for item in population_trend) / len(population_trend)
                if population_trend
                else 0.0
            )
            floating_max = max((item["floating_population"] for item in population_trend), default=0)
            transport_access_index = round((floating_avg / max(float(floating_max), 1.0)) * 100, 1)

            heatmap_source = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            SUM(COALESCE(monday_sales_amount, 0)) AS monday_sales_amount,
                            SUM(COALESCE(tuesday_sales_amount, 0)) AS tuesday_sales_amount,
                            SUM(COALESCE(wednesday_sales_amount, 0)) AS wednesday_sales_amount,
                            SUM(COALESCE(thursday_sales_amount, 0)) AS thursday_sales_amount,
                            SUM(COALESCE(friday_sales_amount, 0)) AS friday_sales_amount,
                            SUM(COALESCE(saturday_sales_amount, 0)) AS saturday_sales_amount,
                            SUM(COALESCE(sunday_sales_amount, 0)) AS sunday_sales_amount,
                            SUM(COALESCE(time_06_11_sales_amount, 0)) AS time_06_11_sales_amount,
                            SUM(COALESCE(time_11_14_sales_amount, 0)) AS time_11_14_sales_amount,
                            SUM(COALESCE(time_14_17_sales_amount, 0)) AS time_14_17_sales_amount,
                            SUM(COALESCE(time_17_21_sales_amount, 0)) AS time_17_21_sales_amount
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .one()
            )
            day_values = {
                "월": float(heatmap_source["monday_sales_amount"] or 0),
                "화": float(heatmap_source["tuesday_sales_amount"] or 0),
                "수": float(heatmap_source["wednesday_sales_amount"] or 0),
                "목": float(heatmap_source["thursday_sales_amount"] or 0),
                "금": float(heatmap_source["friday_sales_amount"] or 0),
                "토": float(heatmap_source["saturday_sales_amount"] or 0),
                "일": float(heatmap_source["sunday_sales_amount"] or 0),
            }
            hour_values = {
                "08-11": float(heatmap_source["time_06_11_sales_amount"] or 0),
                "11-14": float(heatmap_source["time_11_14_sales_amount"] or 0),
                "14-18": float(heatmap_source["time_14_17_sales_amount"] or 0),
                "18-22": float(heatmap_source["time_17_21_sales_amount"] or 0),
            }
            day_total = max(sum(day_values.values()), 1.0)
            hour_total = max(sum(hour_values.values()), 1.0)
            sales_heatmap: list[dict] = []
            for day_label, day_amount in day_values.items():
                for hour_band, hour_amount in hour_values.items():
                    score = ((day_amount / day_total) * 0.55 + (hour_amount / hour_total) * 0.45) * 160
                    sales_heatmap.append(
                        {
                            "dow_label": day_label,
                            "hour_band": hour_band,
                            "sales_index": max(0, min(int(round(score)), 100)),
                        }
                    )

            trend_rows = (
                conn.execute(
                    text(
                        f"""
                        WITH sales_quarter AS (
                            SELECT
                                base_year,
                                base_quarter,
                                SUM(COALESCE(monthly_sales_amount, 0)) AS sales_amount
                            FROM raw_seoul_market_sales
                            WHERE {sales_where}
                            GROUP BY base_year, base_quarter
                        ),
                        pop_quarter AS (
                            SELECT
                                base_year,
                                base_quarter,
                                SUM(COALESCE(total_population, 0)) AS floating_population
                            FROM raw_seoul_market_floating_population
                            WHERE {pop_where}
                            GROUP BY base_year, base_quarter
                        )
                        SELECT
                            COALESCE(p.base_year, s.base_year) AS base_year,
                            COALESCE(p.base_quarter, s.base_quarter) AS base_quarter,
                            COALESCE(p.floating_population, 0) AS floating_population,
                            COALESCE(s.sales_amount, 0) AS sales_amount
                        FROM pop_quarter p
                        FULL OUTER JOIN sales_quarter s
                          ON p.base_year = s.base_year
                         AND p.base_quarter = s.base_quarter
                        ORDER BY base_year, base_quarter
                        """
                    ),
                    {**sales_params, **pop_params},
                )
                .mappings()
                .all()
            )
            trend_rows = trend_rows[-6:]
            floating_population_trend = [
                {
                    "month": f"{int(item['base_year'])}-Q{int(item['base_quarter'])}",
                    "floating_population": int(float(item["floating_population"] or 0)),
                    "estimated_sales_amount": float(item["sales_amount"] or 0),
                }
                for item in trend_rows
            ]

            latest_sales = (
                conn.execute(
                    text(
                        f"""
                        SELECT
                            SUM(COALESCE(monthly_sales_amount, 0)) AS monthly_sales,
                            AVG(COALESCE(weekend_sales_ratio, 0)) AS weekend_ratio
                        FROM raw_seoul_market_sales
                        WHERE {sales_where}
                        """
                    ),
                    sales_params,
                )
                .mappings()
                .one()
            )
            monthly_sales = float(latest_sales["monthly_sales"] or 0)
            weekend_ratio = float(latest_sales["weekend_ratio"] or 0)

        corr = 0.0
        if len(floating_population_trend) >= 2:
            pop_values = [point["floating_population"] for point in floating_population_trend]
            sales_values = [point["estimated_sales_amount"] for point in floating_population_trend]
            pop_avg = sum(pop_values) / len(pop_values)
            sales_avg = sum(sales_values) / len(sales_values)
            cov = sum((p - pop_avg) * (s - sales_avg) for p, s in zip(pop_values, sales_values))
            pop_var = sum((p - pop_avg) ** 2 for p in pop_values)
            sales_var = sum((s - sales_avg) ** 2 for s in sales_values)
            denom = (pop_var * sales_var) ** 0.5
            corr = (cov / denom) if denom else 0.0

        data_sources = [
            "서울시 우리마을가게 상권분석서비스(상권-추정매출)_2019.csv",
            "서울시 우리마을가게 상권분석서비스(상권-추정유동인구).csv",
            "서울시 유동인구 연령분포 기반 가구구성 추정(1인가구/3인가족)",
            "서울시 상권데이터 기반 업력/소득소비/직장·주거 인구 추정(프록시)",
        ]
        if used_smallshop:
            if smallshop_key_source == "SBIZ_API_COMMERCIAL_MAP_KEY":
                data_sources.append("소상공인시장진흥공단 상가(상권)정보 API (SBIZ_API_COMMERCIAL_MAP_KEY)")
            elif smallshop_key_source == "SBIZ_API_STORE_STATUS_KEY":
                data_sources.append("소상공인365 업소현황 API (SBIZ_API_STORE_STATUS_KEY)")
            else:
                data_sources.append("소상공인시장진흥공단 상가(상권)정보 API (EXTERNAL_API_KEY)")

        return {
            "category_sales_pie": category_sales_pie,
            "competitors": competitors,
            "residential_population_radar": residential_population_radar,
            "household_composition_pie": household_composition_pie,
            "estimated_residence_regions": estimated_residence_regions,
            "estimated_sales_summary": {
                "monthly_estimated_sales": monthly_sales,
                "weekly_estimated_sales": round(monthly_sales / 4.35, 0),
                "weekend_ratio": round(weekend_ratio, 1),
            },
            "sales_heatmap": sales_heatmap,
            "floating_population_trend": floating_population_trend,
            "floating_population_analysis": (
                f"reference CSV(서울시 우리마을가게) 기준 유동인구-매출 상관계수는 {corr:+.2f}입니다."
            ),
            "data_sources": data_sources,
            "smallshop_key_source": smallshop_key_source,
            "industry_analysis": {
                "business_count_trend": industry_business_count_trend,
                "business_age_5y": industry_business_age_5y,
            },
            "sales_analysis": {
                "monthly_sales_trend": sales_monthly_trend,
                "monthly_average_sales": monthly_average_sales,
            },
            "population_analysis": {
                "population_trend": population_trend,
                "income_consumption": income_consumption,
            },
            "regional_status": {
                "household_count": int(round(household_proxy_total)),
                "apartment_household_count": int(round(three_person_proxy)),
                "major_facilities_count": len(competitors),
                "transport_access_index": transport_access_index,
            },
            "customer_characteristics": {
                "male_ratio": round(male_ratio * 100, 1),
                "female_ratio": round((1.0 - male_ratio) * 100, 1),
                "new_customer_ratio": None,
                "regular_customer_ratio": None,
                "top_age_group": top_age_group,
                "top_visit_time": top_visit_time,
            },
        }

    @staticmethod
    def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        lon1_rad, lat1_rad, lon2_rad, lat2_rad = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        h = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
        return 6371.0 * 2 * asin(sqrt(h))

    @staticmethod
    def _resolve_scope_center(gu: str | None, dong: str | None) -> tuple[float, float]:
        if dong and dong in _DONG_CENTER:
            return _DONG_CENTER[dong]
        if gu and gu in _GU_CENTER:
            return _GU_CENTER[gu]
        return _DEFAULT_CENTER

    @staticmethod
    def _looks_like_target_industry(item: dict, industry: str | None) -> bool:
        inds_text = " ".join(
            [
                str(item.get("indsLclsNm") or ""),
                str(item.get("indsMclsNm") or ""),
                str(item.get("indsSclsNm") or ""),
            ]
        )
        target_tokens = ("제과", "베이커리", "커피", "카페", "디저트")
        if not any(token in inds_text for token in target_tokens):
            return False
        if not industry:
            return True
        normalized = industry.strip()
        if not normalized:
            return True
        return normalized in inds_text

    def _fetch_smallshop_competitors(
        self,
        *,
        gu: str | None,
        dong: str | None,
        industry: str | None,
        radius_km: float,
    ) -> tuple[list[dict], str | None]:
        service_key, key_source = self._resolve_smallshop_service_key()
        if not service_key:
            return [], None

        center_lon, center_lat = self._resolve_scope_center(gu=gu, dong=dong)
        params = {
            "serviceKey": service_key,
            "pageNo": 1,
            "numOfRows": 1000,
            "radius": max(int(round(radius_km * 1000)), 300),
            "cx": center_lon,
            "cy": center_lat,
        }

        try:
            with httpx.Client(timeout=8.0) as client:
                response = client.get(_SMALLSHOP_RADIUS_URL, params=params)
                response.raise_for_status()
            root = ET.fromstring(response.text)
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("SmallShop API 호출 실패: %s", exc)
            return [], key_source

        result_code = (root.findtext("./header/resultCode") or "").strip()
        if result_code not in {"00", "03"}:
            result_msg = (root.findtext("./header/resultMsg") or "").strip()
            logger.warning("SmallShop API 오류: code=%s msg=%s", result_code, result_msg)
            return [], key_source
        if result_code == "03":
            return [], key_source

        items = root.findall("./body/items/item")
        competitors: list[dict] = []
        for node in items:
            item = {
                "bizesNm": (node.findtext("bizesNm") or "").strip(),
                "brchNm": (node.findtext("brchNm") or "").strip(),
                "indsLclsNm": (node.findtext("indsLclsNm") or "").strip(),
                "indsMclsNm": (node.findtext("indsMclsNm") or "").strip(),
                "indsSclsNm": (node.findtext("indsSclsNm") or "").strip(),
                "lon": (node.findtext("lon") or "").strip(),
                "lat": (node.findtext("lat") or "").strip(),
                "rdnmAdr": (node.findtext("rdnmAdr") or "").strip(),
            }
            if not self._looks_like_target_industry(item, industry):
                continue
            try:
                lon = float(item["lon"])
                lat = float(item["lat"])
            except (TypeError, ValueError):
                continue

            distance_km = self._haversine_km(center_lon, center_lat, lon, lat)
            if distance_km > radius_km:
                continue

            brand_name = item["bizesNm"] or item["indsSclsNm"] or "상가"
            branch = item["brchNm"]
            store_name = f"{brand_name} {branch}".strip() if branch else brand_name
            competitors.append(
                {
                    "brand_name": brand_name,
                    "store_name": store_name,
                    "distance_km": round(distance_km, 2),
                    "industry": item["indsSclsNm"] or item["indsMclsNm"] or item["indsLclsNm"],
                }
            )

        dedup: dict[str, dict] = {}
        for item in sorted(competitors, key=lambda x: x["distance_km"]):
            key = f"{item['store_name']}|{item['industry']}"
            if key not in dedup:
                dedup[key] = item
            if len(dedup) >= 10:
                break
        return list(dedup.values()), key_source

    @staticmethod
    def _resolve_smallshop_service_key() -> tuple[str | None, str | None]:
        external_key = unquote((settings.EXTERNAL_API_KEY or "").strip())
        if external_key and external_key != "stub-key":
            return external_key, "EXTERNAL_API_KEY"

        sbiz_commercial_map_key = unquote((settings.SBIZ_API_COMMERCIAL_MAP_KEY or "").strip())
        if sbiz_commercial_map_key:
            return sbiz_commercial_map_key, "SBIZ_API_COMMERCIAL_MAP_KEY"

        return None, None

    @staticmethod
    def _build_sbiz_store_reports(
        *,
        live_status_overrides: dict[str, str] | None = None,
    ) -> list[dict]:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        api_rows = [
            ("snsAnaly", "SNS 분석", settings.SBIZ_API_SNS_ANALYSIS_KEY),
            ("weather", "창업기상도", settings.SBIZ_API_STARTUP_WEATHER_KEY),
            ("hpReport", "핫플레이스", settings.SBIZ_API_HOTPLACE_KEY),
            ("slsIdex", "점포당 매출액 추이", settings.SBIZ_API_SALES_INDEX_KEY),
            ("stcarSttus", "업력현황", settings.SBIZ_API_BUSINESS_DURATION_KEY),
            ("storSttus", "업소현황", settings.SBIZ_API_STORE_STATUS_KEY),
            ("startupPublic", "상권지도", settings.SBIZ_API_COMMERCIAL_MAP_KEY),
            ("detail", "상세분석", settings.SBIZ_API_DETAIL_ANALYSIS_KEY),
            ("delivery", "배달분석", settings.SBIZ_API_DELIVERY_ANALYSIS_KEY),
            ("tour", "관광 축제 정보", settings.SBIZ_API_TOUR_FESTIVAL_KEY),
            ("simple", "간단분석", settings.SBIZ_API_SIMPLE_ANALYSIS_KEY),
        ]

        reports: list[dict] = []
        for api_code, api_name, cert_key in api_rows:
            has_key = bool((cert_key or "").strip())
            status = "실호출 미확인" if has_key else "키 미설정"
            if live_status_overrides and api_code in live_status_overrides:
                status = live_status_overrides[api_code]
            reports.append(
                {
                    "report_id": f"SBIZ-{api_code}",
                    "title": f"{api_name} API 연동 상태",
                    "period": "임시 승인기간 내",
                    "generated_at": today,
                    "status": status,
                }
            )
        return reports

    def _collect_sbiz_live_status_overrides(self, *, gu: str | None) -> dict[str, str]:
        statuses: dict[str, str] = {}
        sns_status = self._probe_sbiz_sns_analysis()
        if sns_status:
            statuses["snsAnaly"] = sns_status
        hotplace_status = self._probe_sbiz_hotplace(gu=gu)
        if hotplace_status:
            statuses["hpReport"] = hotplace_status
        delivery_status = self._probe_sbiz_delivery(gu=gu)
        if delivery_status:
            statuses["delivery"] = delivery_status
        tour_status = self._probe_sbiz_tour_page()
        if tour_status:
            statuses["tour"] = tour_status
        store_status = self._probe_sbiz_store_status(gu=gu)
        if store_status:
            statuses["storSttus"] = store_status
        return statuses

    def _probe_sbiz_sns_analysis(self) -> str | None:
        cert_key = (settings.SBIZ_API_SNS_ANALYSIS_KEY or "").strip()
        if not cert_key:
            return None
        try:
            with httpx.Client(timeout=6.0) as client:
                response = client.get(
                    "https://bigdata.sbiz.or.kr/gis/api/snsAnls/getSnsAnlsDetail",
                    params={"lat": "37.5665", "lng": "126.9780"},
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
                if response.status_code >= 400:
                    return "점검 필요"
                payload = response.json()
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 SNS 분석 API 상태 점검 실패: %s", exc)
            return "점검 필요"

        if isinstance(payload, dict) and "returnCode" in payload:
            return "연동중"
        return "점검 필요"

    def _probe_sbiz_hotplace(self, *, gu: str | None) -> str | None:
        cert_key = (settings.SBIZ_API_HOTPLACE_KEY or "").strip()
        if not cert_key:
            return None

        area_cd = "11"
        if gu == "광진구":
            area_cd = "1121"
        elif gu == "마포구":
            area_cd = "1144"
        elif gu == "강남구":
            area_cd = "1168"
        elif gu == "송파구":
            area_cd = "1171"
        elif gu == "영등포구":
            area_cd = "1156"

        params = {
            "sprTypeNo": "1",
            "areaCd": area_cd,
            "upjongGb": "1",
            "upjongCd": "",
            "kind": "area",
        }
        try:
            with httpx.Client(timeout=6.0) as client:
                response = client.post(
                    "https://bigdata.sbiz.or.kr/sbiz/api/bizonSttus/DongMTpctdCmpr/search.json",
                    data=params,
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 핫플레이스 API 상태 점검 실패: %s", exc)
            return "점검 필요"
        return "연동중" if response.status_code < 400 else "점검 필요"

    def _probe_sbiz_delivery(self, *, gu: str | None) -> str | None:
        cert_key = (settings.SBIZ_API_DELIVERY_ANALYSIS_KEY or "").strip()
        if not cert_key:
            return None

        area_cd = "11"
        if gu == "광진구":
            area_cd = "1121"
        elif gu == "마포구":
            area_cd = "1144"
        elif gu == "강남구":
            area_cd = "1168"
        elif gu == "송파구":
            area_cd = "1171"
        elif gu == "영등포구":
            area_cd = "1156"

        params = {
            "sprTypeNo": "1",
            "areaCd": area_cd,
            "upjongGb": "1",
            "upjongCd": "",
            "kind": "area",
        }
        try:
            with httpx.Client(timeout=6.0) as client:
                response = client.post(
                    "https://bigdata.sbiz.or.kr/sbiz/api/bizonSttus/baeminIdex/search.json",
                    data=params,
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 배달분석 API 상태 점검 실패: %s", exc)
            return "점검 필요"
        return "연동중" if response.status_code < 400 else "점검 필요"

    def _probe_sbiz_tour_page(self) -> str | None:
        cert_key = (settings.SBIZ_API_TOUR_FESTIVAL_KEY or "").strip()
        if not cert_key:
            return None
        try:
            with httpx.Client(timeout=6.0) as client:
                response = client.get(
                    "https://bigdata.sbiz.or.kr/gis/tour",
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 관광 API 상태 점검 실패: %s", exc)
            return "점검 필요"
        return "연동중" if response.status_code < 400 else "점검 필요"

    def _probe_sbiz_store_status(self, *, gu: str | None) -> str | None:
        cert_key = (settings.SBIZ_API_STORE_STATUS_KEY or "").strip()
        if not cert_key:
            return None

        area_cd = "11"
        if gu == "광진구":
            area_cd = "1121"
        elif gu == "마포구":
            area_cd = "1144"
        elif gu == "강남구":
            area_cd = "1168"
        elif gu == "송파구":
            area_cd = "1171"
        elif gu == "영등포구":
            area_cd = "1156"

        params = {
            "sprTypeNo": "1",
            "areaCd": area_cd,
            "upjongGb": "1",
            "upjongCd": "",
            "kind": "area",
        }

        try:
            with httpx.Client(timeout=6.0) as client:
                response = client.get(
                    "https://bigdata.sbiz.or.kr/sbiz/api/bizonSttus/storSttus/search.json",
                    params=params,
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
                if response.status_code >= 400:
                    return "점검 필요"
                payload = response.json()
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 업소현황 API 상태 점검 실패: %s", exc)
            return "점검 필요"

        rows = payload.get("upsoList")
        if isinstance(rows, list):
            return "연동중"
        return "점검 필요"

    def _fetch_sbiz_sales_index_summary(
        self,
        *,
        gu: str | None,
    ) -> tuple[dict | None, str | None]:
        cert_key = (settings.SBIZ_API_SALES_INDEX_KEY or "").strip()
        if not cert_key:
            return None, None

        area_cd = "11"
        if gu == "광진구":
            area_cd = "1121"
        elif gu == "마포구":
            area_cd = "1144"
        elif gu == "강남구":
            area_cd = "1168"
        elif gu == "송파구":
            area_cd = "1171"
        elif gu == "영등포구":
            area_cd = "1156"

        params = {
            "sprTypeNo": "1",
            "areaCd": area_cd,
            "upjongGb": "1",
            "upjongCd": "",
            "kind": "area",
        }

        try:
            with httpx.Client(timeout=6.0) as client:
                response = client.get(
                    "https://bigdata.sbiz.or.kr/sbiz/api/bizonSttus/slsIdex/search.json",
                    params=params,
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
                if response.status_code >= 400:
                    logger.warning(
                        "소진공 점포당 매출액 추이 API 응답 오류: status=%s",
                        response.status_code,
                    )
                    return None, "점검 필요"
                payload = response.json()
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 점포당 매출액 추이 API 호출 실패: %s", exc)
            return None, "점검 필요"

        candidates = []
        for key in ("slsList", "mapList", "upsoList", "resultList"):
            rows = payload.get(key)
            if isinstance(rows, list):
                candidates = [row for row in rows if isinstance(row, dict)]
                if candidates:
                    break
        if not candidates:
            return None, "키 설정됨"

        filtered_rows = candidates
        if gu:
            scoped = [
                row for row in candidates
                if gu in str(row.get("areaNm") or row.get("area_name") or "")
            ]
            if scoped:
                filtered_rows = scoped

        def _to_float(value: object) -> float:
            try:
                return float(value or 0)
            except (TypeError, ValueError):
                return 0.0

        monthly_candidates = (
            "aSum",
            "saleAmt",
            "salesAmt",
            "monthlySales",
            "totSaleAmt",
            "slsAmt",
        )
        weekend_ratio_candidates = (
            "weekendRatio",
            "weekendPer",
            "weekendSalesRatio",
        )

        best_monthly = 0.0
        weekend_ratio = 0.0
        for row in filtered_rows:
            monthly = max(_to_float(row.get(key)) for key in monthly_candidates)
            if monthly > best_monthly:
                best_monthly = monthly
                weekend_ratio = max(_to_float(row.get(key)) for key in weekend_ratio_candidates)

        if best_monthly <= 0:
            return None, "키 설정됨"

        return {
            "monthly_estimated_sales": round(best_monthly, 0),
            "weekly_estimated_sales": round(best_monthly / 4.35, 0),
            "weekend_ratio": round(weekend_ratio, 1) if weekend_ratio > 0 else 0.0,
        }, "연동중"

    def _fetch_sbiz_store_status_competitors(
        self,
        *,
        gu: str | None,
        dong: str | None,
        radius_km: float,
    ) -> tuple[list[dict], str | None]:
        cert_key = (settings.SBIZ_API_STORE_STATUS_KEY or "").strip()
        if not cert_key:
            return [], None

        center_lon, center_lat = self._resolve_scope_center(gu=gu, dong=dong)
        area_cd = "11"
        if gu == "광진구":
            area_cd = "1121"
        elif gu == "마포구":
            area_cd = "1144"
        elif gu == "강남구":
            area_cd = "1168"
        elif gu == "송파구":
            area_cd = "1171"
        elif gu == "영등포구":
            area_cd = "1156"

        params = {
            "sprTypeNo": "1",
            "areaCd": area_cd,
            "upjongGb": "1",
            "upjongCd": "",
            "kind": "area",
        }

        try:
            with httpx.Client(timeout=8.0) as client:
                response = client.get(
                    "https://bigdata.sbiz.or.kr/sbiz/api/bizonSttus/storSttus/search.json",
                    params=params,
                    headers={"Cookie": f"XTLOGINID={cert_key}"},
                )
                response.raise_for_status()
                payload = response.json()
        except Exception as exc:  # noqa: BLE001 - 외부 API 장애 시 fallback 유지
            logger.warning("소진공 업소현황 API 호출 실패: %s", exc)
            return [], "SBIZ_API_STORE_STATUS_KEY"

        rows = payload.get("upsoList")
        if not isinstance(rows, list):
            return [], "SBIZ_API_STORE_STATUS_KEY"

        normalized: list[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            area_name = str(row.get("areaNm") or "").strip()
            if not area_name:
                continue
            area_level = str(row.get("areaGb") or "")
            if area_level not in {"12", "13", "24"}:
                continue
            if gu and gu not in area_name:
                if area_level == "12":
                    continue
            try:
                upso_count = float(row.get("aSum") or 0)
            except (TypeError, ValueError):
                upso_count = 0.0
            try:
                updown_rate = float(row.get("updownPer") or 0)
            except (TypeError, ValueError):
                updown_rate = 0.0
            normalized.append(
                {
                    "area_name": area_name,
                    "upso_count": upso_count,
                    "updown_rate": updown_rate,
                }
            )

        if not normalized:
            return [], "SBIZ_API_STORE_STATUS_KEY"

        normalized.sort(key=lambda item: item["upso_count"], reverse=True)

        competitors: list[dict] = []
        for idx, item in enumerate(normalized[:10], start=1):
            area_name = str(item["area_name"])
            point = _GU_CENTER.get(area_name)
            if point:
                distance_km = self._haversine_km(center_lon, center_lat, point[0], point[1])
            else:
                distance_km = min(radius_km, 0.3 + idx * 0.2)
            competitors.append(
                {
                    "brand_name": "소진공 업소현황",
                    "store_name": area_name,
                    "distance_km": round(float(distance_km), 2),
                    "industry": "업소현황",
                    "trend_direction": (
                        "up"
                        if float(item["updown_rate"]) > 0
                        else "down"
                        if float(item["updown_rate"]) < 0
                        else "flat"
                    ),
                }
            )

        return competitors, "SBIZ_API_STORE_STATUS_KEY"

    def _merge_competitor_trends_from_reference(
        self,
        *,
        live_competitors: list[dict],
        fallback_competitors: list[dict],
    ) -> list[dict]:
        if not live_competitors:
            return fallback_competitors
        fallback_map = {
            str(item.get("brand_name", "")): item
            for item in fallback_competitors
        }
        merged: list[dict] = []
        for rank, item in enumerate(live_competitors, start=1):
            fallback = fallback_map.get(str(item.get("industry", ""))) or (
                fallback_competitors[min(rank - 1, len(fallback_competitors) - 1)]
                if fallback_competitors
                else None
            )
            sales_trend = []
            payment_demographics = []
            trend_direction = str(item.get("trend_direction", "flat"))
            if fallback:
                sales_trend = list(fallback.get("sales_trend", []))
                payment_demographics = list(fallback.get("payment_demographics", []))
                fallback_trend = str(fallback.get("trend_direction", "")).strip()
                if fallback_trend:
                    trend_direction = fallback_trend
            merged.append(
                {
                    "rank": rank,
                    "brand_name": str(item.get("brand_name", "")),
                    "store_name": str(item.get("store_name", "")),
                    "distance_km": float(item.get("distance_km", 0)),
                    "trend_direction": trend_direction,
                    "sales_trend": sales_trend,
                    "payment_demographics": payment_demographics,
                }
            )
        return merged
