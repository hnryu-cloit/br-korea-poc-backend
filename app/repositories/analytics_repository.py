from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table


class AnalyticsRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def get_metrics(self) -> list[dict]:
        if not self.engine:
            return []

        items: list[dict] = []
        try:
            channel_metrics = self._get_channel_metrics()
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
            sales_metrics = self._get_sales_metrics()
        except SQLAlchemyError:
            sales_metrics = None
        if sales_metrics:
            items.extend([sales_metrics["total_sales"], sales_metrics["coffee_attach_ratio"]])

        try:
            discount_metrics = self._get_discount_metrics()
        except SQLAlchemyError:
            discount_metrics = None
        if discount_metrics:
            items.append(discount_metrics["discount_ratio"])

        return items

    def _get_channel_metrics(self) -> dict | None:
        if not has_table(self.engine, "core_channel_sales"):
            return None
        with self.engine.connect() as connection:
            summary = connection.execute(
                text(
                    """
                    WITH daily AS (
                        SELECT
                            sale_dt,
                            SUM(sale_amt) AS total_sales,
                            SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN ord_cnt ELSE 0 END) AS online_orders,
                            SUM(CASE WHEN ho_chnl_div LIKE '오프라인%' THEN ord_cnt ELSE 0 END) AS offline_orders,
                            SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN sale_amt ELSE 0 END) AS online_sales,
                            SUM(ord_cnt) AS total_orders
                        FROM core_channel_sales
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
            ).mappings().one()

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

        app_ratio_recent = 0 if recent_total_sales == 0 else round(recent_online_sales / recent_total_sales * 100, 1)
        app_ratio_prior = 0 if prior_total_sales == 0 else round(prior_online_sales / prior_total_sales * 100, 1)
        avg_ticket_recent = 0 if recent_total_orders == 0 else round(recent_total_sales / recent_total_orders)
        avg_ticket_prior = 0 if prior_total_orders == 0 else round(prior_total_sales / prior_total_orders)

        delivery_change, delivery_trend = fmt_change(recent_online_orders, prior_online_orders)
        hall_change, hall_trend = fmt_change(recent_offline_orders, prior_offline_orders)
        app_ratio_delta = app_ratio_recent - app_ratio_prior
        app_ratio_trend = "up" if app_ratio_delta > 0 else "down" if app_ratio_delta < 0 else "flat"
        avg_ticket_change, avg_ticket_trend = fmt_change(avg_ticket_recent, avg_ticket_prior)

        return {
            "delivery_orders": {
                "label": "배달 건수",
                "value": f"{int(round(recent_online_orders)):,}건",
                "change": delivery_change,
                "trend": delivery_trend,
                "detail": "최근 7일 기준",
            },
            "hall_visits": {
                "label": "홀 방문 고객",
                "value": f"{int(round(recent_offline_orders)):,}명",
                "change": hall_change,
                "trend": hall_trend,
                "detail": "최근 7일 기준",
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
                "detail": "최근 7일 기준",
            },
        }

    def _get_sales_metrics(self) -> dict | None:
        if not has_table(self.engine, "core_daily_item_sales"):
            return None
        with self.engine.connect() as connection:
            summary = connection.execute(
                text(
                    """
                    WITH daily AS (
                        SELECT
                            sale_dt,
                            SUM(net_sale_amt) AS total_sales
                        FROM core_daily_item_sales
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
            ).mappings().one()

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

        coffee_ratio_recent = 0 if coffee_recent_total == 0 else round(recent_coffee_sales / coffee_recent_total * 100, 1)
        coffee_ratio_prior = 0 if coffee_prior_total == 0 else round(prior_coffee_sales / coffee_prior_total * 100, 1)
        coffee_delta = coffee_ratio_recent - coffee_ratio_prior
        coffee_trend = "up" if coffee_delta > 0 else "down" if coffee_delta < 0 else "flat"

        return {
            "total_sales": {
                "label": "이번 주 총 매출",
                "value": f"₩{int(round(recent_total_sales)):,}",
                "change": sales_change,
                "trend": sales_trend,
                "detail": "최근 7일 기준",
            },
            "coffee_attach_ratio": {
                "label": "커피 동반 구매율",
                "value": f"{coffee_ratio_recent:.1f}%",
                "change": f"{coffee_delta:+.1f}%p",
                "trend": coffee_trend,
                "detail": "커피 계열 매출 비중",
            },
        }

    def _get_discount_metrics(self) -> dict | None:
        if not has_table(self.engine, "raw_daily_store_pay_way"):
            return None

        has_settlement = has_table(self.engine, "raw_settlement_master")
        has_policy = has_table(self.engine, "raw_telecom_discount_policy")
        with self.engine.connect() as connection:
            summary = connection.execute(
                text(
                    """
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
            ).mappings().one()

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

        recent_ratio = 0 if recent_total_amt == 0 else round(recent_discount_amt / recent_total_amt * 100, 1)
        prior_ratio = 0 if prior_total_amt == 0 else round(prior_discount_amt / prior_total_amt * 100, 1)
        ratio_delta = round(recent_ratio - prior_ratio, 1)
        trend = "up" if ratio_delta > 0 else "down" if ratio_delta < 0 else "flat"

        detail_parts = ["제휴할인 + 캠페인할인결제"]
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
