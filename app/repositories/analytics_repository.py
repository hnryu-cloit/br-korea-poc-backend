from __future__ import annotations

import logging
import random
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
        profile = self.get_store_profile(store_id=store_id)
        if profile is None:
            profile = {
                "masked_stor_cd": store_id or "STORE_DEMO",
                "maked_stor_nm": "기본매장",
                "sido": "서울",
                "region": "중구",
                "actual_sales_amt": 120000000,
            }

        seed_base = str(profile.get("masked_stor_cd") or store_id or "STORE_DEMO")
        seed = sum(ord(ch) for ch in seed_base)
        rng = random.Random(seed)
        radius_km = round(max(min((radius_m or 3000) / 1000.0, 3.0), 0.1), 2)

        monthly_base_sales = max(float(profile.get("actual_sales_amt") or 0) / 12.0, 8_000_000.0)
        trade_area_total = monthly_base_sales * rng.uniform(8.5, 11.5)
        bakery_ratio = rng.uniform(0.38, 0.56)
        coffee_ratio = 1 - bakery_ratio
        category_sales_pie = [
            {
                "category": "제과",
                "sales_amount": round(trade_area_total * bakery_ratio, 0),
                "share_ratio": round(bakery_ratio * 100, 1),
            },
            {
                "category": "커피",
                "sales_amount": round(trade_area_total * coffee_ratio, 0),
                "share_ratio": round(coffee_ratio * 100, 1),
            },
        ]

        resident_age_groups = ["19세 이하", "20대", "30대", "40대", "50대", "60대 이상"]
        residential_population_radar: list[dict] = []
        for age_group in resident_age_groups:
            cohort_base = int(rng.uniform(900, 3200))
            male_ratio = rng.uniform(0.44, 0.56)
            male_population = int(round(cohort_base * male_ratio))
            female_population = max(cohort_base - male_population, 0)
            residential_population_radar.append(
                {
                    "age_group": age_group,
                    "male_population": male_population,
                    "female_population": female_population,
                }
            )

        one_person_ratio = rng.uniform(0.44, 0.58)
        three_person_ratio = 1.0 - one_person_ratio
        household_base = int(rng.uniform(12000, 29000))
        one_person_count = int(round(household_base * one_person_ratio))
        three_person_count = max(household_base - one_person_count, 0)
        household_composition_pie = [
            {
                "household_type": "1인가구",
                "household_count": one_person_count,
                "share_ratio": round((one_person_count / household_base) * 100, 1),
            },
            {
                "household_type": "3인가족",
                "household_count": three_person_count,
                "share_ratio": round((three_person_count / household_base) * 100, 1),
            },
        ]

        residence_candidates = [
            f"{profile.get('sido', '서울')} {profile.get('region', '중구')}",
            f"{profile.get('sido', '서울')} 마포구",
            f"{profile.get('sido', '서울')} 강서구",
            "경기 고양시",
            "경기 성남시",
        ]
        residence_weights = [rng.uniform(0.14, 0.38) for _ in residence_candidates]
        weight_sum = sum(residence_weights) or 1.0
        normalized_ratios = [weight / weight_sum for weight in residence_weights]
        estimated_residence_regions: list[dict] = []
        for region_name, ratio in zip(residence_candidates, normalized_ratios):
            estimated_residence_regions.append(
                {
                    "region_name": region_name,
                    "share_ratio": round(ratio * 100, 1),
                    "estimated_customers": int(round(ratio * household_base * 1.25)),
                }
            )

        monthly_estimated_sales = round(trade_area_total * rng.uniform(0.84, 1.12), 0)
        estimated_sales_summary = {
            "monthly_estimated_sales": monthly_estimated_sales,
            "weekly_estimated_sales": round(monthly_estimated_sales / 4.35, 0),
            "weekend_ratio": round(rng.uniform(32.0, 48.0), 1),
        }

        dow_labels = ["월", "화", "수", "목", "금", "토", "일"]
        hour_bands = ["08-11", "11-14", "14-18", "18-22"]
        sales_heatmap: list[dict] = []
        for dow in dow_labels:
            for hour_band in hour_bands:
                base_idx = rng.randint(48, 92)
                if dow in ("토", "일"):
                    base_idx += 8
                if hour_band in ("11-14", "18-22"):
                    base_idx += 6
                sales_heatmap.append(
                    {
                        "dow_label": dow,
                        "hour_band": hour_band,
                        "sales_index": max(0, min(base_idx, 100)),
                    }
                )

        current_month = datetime.utcnow().strftime("%Y-%m")
        previous_month = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m")
        store_reports = [
            {
                "report_id": f"RPT-{seed % 10000:04d}-01",
                "title": "구의역 상권분석 2024 | 식당·카페 창업 완전 가이드",
                "period": "2024",
                "generated_at": "2026-03-10",
                "status": "완료",
            },
            {
                "report_id": f"RPT-{seed % 10000:04d}-02",
                "title": "경쟁사 트렌드 비교 리포트",
                "period": previous_month,
                "generated_at": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"),
                "status": "완료",
            },
        ]

        now = datetime.utcnow()
        months = []
        for idx in range(5, -1, -1):
            d = (now.replace(day=15) - timedelta(days=idx * 30))
            months.append(d.strftime("%Y-%m"))

        brand_pool = [
            "던킨",
            "파리바게뜨",
            "뚜레쥬르",
            "스타벅스",
            "투썸플레이스",
            "메가커피",
            "컴포즈커피",
            "빽다방",
            "이디야",
            "할리스",
            "폴바셋",
            "앤티앤스",
        ]
        age_groups = ["10대", "20대", "30대", "40대+",]
        competitors: list[dict] = []
        for rank in range(1, 11):
            brand_name = brand_pool[(seed + rank) % len(brand_pool)]
            store_name = f"{brand_name} {profile.get('region', '상권')} {rank}호점"
            distance_km = round(rng.uniform(0.3, 2.9), 2)
            comp_base = monthly_base_sales * rng.uniform(0.55, 1.45)
            drift = rng.uniform(-0.06, 0.09)
            sales_trend: list[dict] = []
            for month_idx, month in enumerate(months):
                trend_amt = comp_base * (1 + drift * month_idx) * rng.uniform(0.92, 1.08)
                sales_trend.append(
                    {
                        "month": month,
                        "sales_amount": round(max(trend_amt, comp_base * 0.55), 0),
                    }
                )

            first_sales = float(sales_trend[0]["sales_amount"])
            last_sales = float(sales_trend[-1]["sales_amount"])
            trend_direction = "up" if last_sales > first_sales else "down" if last_sales < first_sales else "flat"

            payment_demographics: list[dict] = []
            for age_group in age_groups:
                payment_base = int(rng.uniform(120, 430))
                male_ratio = rng.uniform(0.35, 0.62)
                male_count = int(round(payment_base * male_ratio))
                female_count = max(payment_base - male_count, 0)
                payment_demographics.append(
                    {
                        "age_group": age_group,
                        "male_payment_count": male_count,
                        "female_payment_count": female_count,
                    }
                )

            competitors.append(
                {
                    "rank": rank,
                    "brand_name": brand_name,
                    "store_name": store_name,
                    "distance_km": distance_km,
                    "trend_direction": trend_direction,
                    "sales_trend": sales_trend,
                    "payment_demographics": payment_demographics,
                }
            )

        floating_population_trend: list[dict] = []
        float_base = int(rng.uniform(42000, 98000))
        for month_idx, month in enumerate(months):
            pop = int(float_base * (1 + rng.uniform(-0.09, 0.13) * (month_idx / 3 + 0.5)))
            est_sales = round(pop * rng.uniform(185, 245), 0)
            floating_population_trend.append(
                {
                    "month": month,
                    "floating_population": pop,
                    "estimated_sales_amount": est_sales,
                }
            )

        pop_values = [point["floating_population"] for point in floating_population_trend]
        sales_values = [point["estimated_sales_amount"] for point in floating_population_trend]
        corr = 0.0
        if len(pop_values) >= 2:
            pop_avg = sum(pop_values) / len(pop_values)
            sales_avg = sum(sales_values) / len(sales_values)
            cov = sum((p - pop_avg) * (s - sales_avg) for p, s in zip(pop_values, sales_values))
            pop_var = sum((p - pop_avg) ** 2 for p in pop_values)
            sales_var = sum((s - sales_avg) ** 2 for s in sales_values)
            denom = (pop_var * sales_var) ** 0.5
            corr = (cov / denom) if denom else 0.0

        analysis = (
            f"서울시 공공데이터 유동인구 추세와 상권 추정 매출의 상관계수는 {corr:+.2f}입니다. "
            "유동인구 증가 월에는 제과/커피 동반 성장 경향이 확인되어, 프로모션과 재고 배치를 주말·퇴근 시간대에 강화하는 전략이 유효합니다."
        )

        data_sources = [
            "내부 매출 데이터(core/raw 매출 테이블)",
            "오픈업 결제건 연령/성별 요약 데이터(POC 가공)",
            "서울시 공공데이터 유동인구(월별 집계, POC 샘플 반영)",
            "서울시 공공데이터 주거인구/가구구성(POC 샘플 반영)",
        ]

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
            estimated_sales_summary = (
                reference_payload.get("estimated_sales_summary") or estimated_sales_summary
            )
            sales_heatmap = reference_payload.get("sales_heatmap") or sales_heatmap
            floating_population_trend = (
                reference_payload.get("floating_population_trend") or floating_population_trend
            )
            analysis = reference_payload.get("floating_population_analysis") or analysis
            data_sources = reference_payload.get("data_sources") or data_sources

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
                return None
            category_sales_pie = [
                {
                    "category": str(row["category"]),
                    "sales_amount": float(row["sales_amount"] or 0),
                    "share_ratio": round(float(row["sales_amount"] or 0) / category_total * 100, 1),
                }
                for row in category_rows
            ]

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

            live_competitors = self._fetch_smallshop_competitors(
                gu=scope_gu,
                dong=scope_dong,
                industry=scope_industry,
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
                            SUM(COALESCE(age60_plus_population, 0)) AS age60_plus_population
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
        ]
        if used_smallshop:
            data_sources.append("소상공인시장진흥공단 상가(상권)정보 API (공공데이터포털)")

        return {
            "category_sales_pie": category_sales_pie,
            "competitors": competitors,
            "residential_population_radar": residential_population_radar,
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
    ) -> list[dict]:
        service_key = unquote((settings.EXTERNAL_API_KEY or "").strip())
        if not service_key or service_key == "stub-key":
            return []

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
            return []

        result_code = (root.findtext("./header/resultCode") or "").strip()
        if result_code not in {"00", "03"}:
            result_msg = (root.findtext("./header/resultMsg") or "").strip()
            logger.warning("SmallShop API 오류: code=%s msg=%s", result_code, result_msg)
            return []
        if result_code == "03":
            return []

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
        return list(dedup.values())

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
            trend_direction = "flat"
            if fallback:
                sales_trend = list(fallback.get("sales_trend", []))
                payment_demographics = list(fallback.get("payment_demographics", []))
                trend_direction = str(fallback.get("trend_direction", "flat"))
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
