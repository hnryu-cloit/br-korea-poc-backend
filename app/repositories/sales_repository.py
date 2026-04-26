from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date as date_type
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config.store_mart_mapping import get_store_mart_table
from app.core.utils import get_now
from app.infrastructure.db.utils import has_table
from app.repositories.sales.campaign_repository import CampaignRepositoryMixin
from app.repositories.sales.insight_repository import InsightRepositoryMixin
from app.repositories.sales.prompt_repository import PromptRepositoryMixin

logger = logging.getLogger(__name__)


class SalesRepository(PromptRepositoryMixin, InsightRepositoryMixin, CampaignRepositoryMixin):
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine
        self._workbook_sheet_cache: dict[str, list[dict]] = {}

    async def get_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        """오늘 매출 요약 및 최근 7일 주간 데이터, 상품별 매출 순위를 실 DB에서 집계"""
        if not self.engine:
            raise RuntimeError("sales database engine is not configured")

        result: dict = {
            "data_date": None,
            "today_revenue": 0.0,
            "today_net_revenue": 0.0,
            "weekly_data": [],
            "top_products": [],
            "group_revenue_share": [],
            "avg_margin_rate": 0.0,
            "avg_net_profit_per_item": 0.0,
            "avg_ticket_size": 0.0,
            "avg_ticket_index": 0.0,
            "estimated_today_profit": 0.0,
        }

        # 1. 사용할 테이블·컬럼 결정
        item_relation = "raw_daily_store_item"
        amt_col = "sale_amt"
        net_col = "net_sale_amt"
        qty_col = "sale_qty"

        normalized_date_from = date_from.replace("-", "") if date_from else None
        normalized_date_to = date_to.replace("-", "") if date_to else None

        store_filter = ""
        params: dict = {}
        if store_id:
            store_filter = "AND masked_stor_cd = :store_id"
            params["store_id"] = store_id
        if normalized_date_from:
            store_filter += " AND sale_dt >= :date_from"
            params["date_from"] = normalized_date_from
        if normalized_date_to:
            store_filter += " AND sale_dt <= :date_to"
            params["date_to"] = normalized_date_to

        try:
            with self.engine.connect() as connection:
                # 2. 필터 내 최신 날짜 조회
                date_bounds_row = (
                    connection.execute(
                        text(
                            f"SELECT MAX(sale_dt) AS max_dt "
                            f"FROM {item_relation} "
                            f"WHERE sale_dt IS NOT NULL {store_filter}"
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
                if not date_bounds_row or not date_bounds_row["max_dt"]:
                    raise LookupError("매출 요약 데이터가 없습니다.")
                max_dt = str(date_bounds_row["max_dt"])
                result["data_date"] = max_dt

                # 3. 오늘(최신일) 매출 집계
                analytics_daily_table = get_store_mart_table(store_id, "analytics", "daily_table")
                today_row = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST({amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS revenue,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST({net_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS net_revenue
                        FROM {item_relation}
                        WHERE sale_dt = :max_dt {store_filter}
                        """
                        ),
                        {**params, "max_dt": max_dt},
                    )
                    .mappings()
                    .first()
                )
                if today_row:
                    result["today_revenue"] = float(today_row["revenue"] or 0)
                    result["today_net_revenue"] = float(today_row["net_revenue"] or 0)
                    bill_count = 0.0
                    if analytics_daily_table and has_table(self.engine, analytics_daily_table):
                        ticket_row = (
                            connection.execute(
                                text(
                                    f"""
                                    SELECT COALESCE(total_order_count, 0) AS total_order_count
                                    FROM {analytics_daily_table}
                                    WHERE sale_dt = :max_dt
                                    """
                                ),
                                {"max_dt": max_dt},
                            )
                            .mappings()
                            .first()
                        )
                        if ticket_row:
                            bill_count = float(ticket_row["total_order_count"] or 0)
                    if bill_count <= 0 and has_table(self.engine, "core_channel_sales"):
                        channel_row = (
                            connection.execute(
                                text(
                                    """
                                    SELECT COALESCE(SUM(COALESCE(NULLIF(TRIM(CAST(ord_cnt AS TEXT)), '')::numeric, 0)), 0) AS total_order_count
                                    FROM core_channel_sales
                                    WHERE masked_stor_cd = :store_id
                                      AND sale_dt = :max_dt
                                    """
                                ),
                                {"store_id": store_id, "max_dt": max_dt},
                            )
                            .mappings()
                            .first()
                        )
                        if channel_row:
                            bill_count = float(channel_row["total_order_count"] or 0)
                    if bill_count > 0:
                        result["avg_ticket_size"] = round(result["today_revenue"] / bill_count, 2)
                        ticket_distribution = self._get_avg_ticket_distribution(
                            connection=connection,
                            store_id=store_id,
                            target_date=max_dt,
                        )
                        result["avg_ticket_index"] = round(
                            self._normalize_index_from_distribution(
                                current_value=result["avg_ticket_size"],
                                values=ticket_distribution,
                            ),
                            1,
                        )

                # 4. 선택 기간 전체 일자별 집계
                if normalized_date_from or normalized_date_to:
                    weekly_query = text(
                        f"""
                        SELECT
                            sale_dt,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST({amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS revenue,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST({net_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS net_revenue
                        FROM {item_relation}
                        WHERE sale_dt IS NOT NULL {store_filter}
                        GROUP BY sale_dt
                        ORDER BY sale_dt ASC
                        """
                    )
                else:
                    weekly_query = text(
                        f"""
                        WITH recent_dates AS (
                            SELECT sale_dt
                            FROM {item_relation}
                            WHERE sale_dt IS NOT NULL {store_filter}
                            GROUP BY sale_dt
                            ORDER BY sale_dt DESC
                            LIMIT 7
                        )
                        SELECT
                            i.sale_dt,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(i.{amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS revenue,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(i.{net_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS net_revenue
                        FROM {item_relation} i
                        JOIN recent_dates d ON i.sale_dt = d.sale_dt
                        WHERE 1=1 {store_filter}
                        GROUP BY i.sale_dt
                        ORDER BY i.sale_dt ASC
                        """
                    )
                weekly_rows = (
                    connection.execute(weekly_query, params).mappings().all()
                )

                result["weekly_data"] = [
                    {
                        "sale_dt": str(row["sale_dt"]),
                        "revenue": float(row["revenue"] or 0),
                        "net_revenue": float(row["net_revenue"] or 0),
                    }
                    for row in weekly_rows
                ]

                # 5. 상품별 매출 순위 (전체 기간 기준)
                product_rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '기타') AS item_nm,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST({amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS sales,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST({qty_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS qty
                        FROM {item_relation}
                        WHERE sale_dt IS NOT NULL {store_filter}
                        GROUP BY item_nm
                        ORDER BY sales DESC, qty DESC
                        LIMIT 6
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
                result["top_products"] = [
                    {
                        "name": str(row["item_nm"]),
                        "sales": float(row["sales"] or 0),
                        "qty": float(row["qty"] or 0),
                    }
                    for row in product_rows
                ]

                # 6. 원가 데이터 기반 평균 마진율·순이익 계산 (raw_production_extract)
                category_expr = "'기타'"
                category_join = ""
                if has_table(self.engine, "mart_item_category_master"):
                    category_expr = "COALESCE(NULLIF(TRIM(CAST(c.category AS TEXT)), ''), '기타')"
                    category_join = """
                        LEFT JOIN (
                            SELECT
                                COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '') AS item_nm,
                                MAX(COALESCE(NULLIF(TRIM(CAST(category AS TEXT)), ''), '기타')) AS category
                            FROM mart_item_category_master
                            GROUP BY COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '')
                        ) c
                          ON c.item_nm = COALESCE(NULLIF(TRIM(CAST(s.item_nm AS TEXT)), ''), '')
                    """
                elif has_table(self.engine, "raw_product_shelf_life"):
                    category_expr = "COALESCE(NULLIF(TRIM(CAST(c.item_group AS TEXT)), ''), '기타')"
                    category_join = """
                        LEFT JOIN (
                            SELECT
                                COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '') AS item_nm,
                                MAX(COALESCE(NULLIF(TRIM(CAST(item_group AS TEXT)), ''), '기타')) AS item_group
                            FROM raw_product_shelf_life
                            GROUP BY COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '')
                        ) c
                          ON c.item_nm = COALESCE(NULLIF(TRIM(CAST(s.item_nm AS TEXT)), ''), '')
                    """

                group_rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            {category_expr} AS group_name,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(s.{amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS sales
                        FROM {item_relation} s
                        {category_join}
                        WHERE s.sale_dt IS NOT NULL {store_filter}
                        GROUP BY group_name
                        ORDER BY sales DESC, group_name ASC
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
                result["group_revenue_share"] = [
                    {
                        "name": str(row["group_name"] or "기타"),
                        "sales": float(row["sales"] or 0),
                    }
                    for row in group_rows
                    if float(row["sales"] or 0) > 0
                ]

                if (
                    len(result["group_revenue_share"]) == 1
                    and str(result["group_revenue_share"][0].get("name") or "") in {"湲고?", "기타"}
                    and has_table(self.engine, "mart_item_category_master")
                ):
                    remapped_group_rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                COALESCE(NULLIF(TRIM(CAST(c.category AS TEXT)), ''), '기타') AS group_name,
                                COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(s.{amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS sales
                            FROM {item_relation} s
                            LEFT JOIN LATERAL (
                                SELECT MAX(COALESCE(NULLIF(TRIM(CAST(m.category AS TEXT)), ''), '기타')) AS category
                                FROM mart_item_category_master m
                                WHERE (
                                    COALESCE(NULLIF(TRIM(CAST(m.item_cd AS TEXT)), ''), '') <> ''
                                    AND COALESCE(NULLIF(TRIM(CAST(m.item_cd AS TEXT)), ''), '') =
                                        COALESCE(NULLIF(TRIM(CAST(s.item_cd AS TEXT)), ''), '')
                                )
                                OR COALESCE(NULLIF(TRIM(CAST(m.item_nm AS TEXT)), ''), '') =
                                   COALESCE(NULLIF(TRIM(CAST(s.item_nm AS TEXT)), ''), '')
                                OR (
                                    COALESCE(NULLIF(TRIM(CAST(m.parent_item_nm AS TEXT)), ''), '') <> ''
                                    AND COALESCE(NULLIF(TRIM(CAST(m.parent_item_nm AS TEXT)), ''), '') =
                                        COALESCE(NULLIF(TRIM(CAST(s.item_nm AS TEXT)), ''), '')
                                )
                            ) c ON TRUE
                            WHERE s.sale_dt IS NOT NULL {store_filter}
                            GROUP BY group_name
                            ORDER BY sales DESC, group_name ASC
                            """
                            ),
                            params,
                        )
                        .mappings()
                        .all()
                    )
                    result["group_revenue_share"] = [
                        {
                            "name": str(row["group_name"] or "기타"),
                            "sales": float(row["sales"] or 0),
                        }
                        for row in remapped_group_rows
                        if float(row["sales"] or 0) > 0
                    ]

                margin_target_dt = normalized_date_to or max_dt
                margin_row = self._get_sales_margin_snapshot(
                    connection=connection,
                    store_id=store_id,
                    target_date=margin_target_dt,
                )
                if margin_row and margin_row.get("avg_margin_rate") is not None:
                    avg_margin = float(margin_row["avg_margin_rate"] or 0)
                    avg_net = float(margin_row.get("avg_net_profit_per_item") or 0)
                    result["avg_margin_rate"] = round(avg_margin, 4)
                    result["avg_net_profit_per_item"] = round(avg_net, 2)
                    result["estimated_today_profit"] = round(
                        result["today_revenue"] * avg_margin, 2
                    )
                elif result["today_revenue"] > 0 and result["today_net_revenue"] > 0:
                    result["avg_margin_rate"] = round(
                        result["today_net_revenue"] / max(result["today_revenue"], 1),
                        4,
                    )
                    result["estimated_today_profit"] = round(result["today_net_revenue"], 2)
                    total_qty = sum(float(item.get("qty") or 0) for item in result["top_products"])
                    if total_qty > 0:
                        result["avg_net_profit_per_item"] = round(
                            result["today_net_revenue"] / total_qty,
                            2,
                        )

        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to aggregate sales summary (store_id=%s, date_from=%s, date_to=%s): %s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            raise RuntimeError("매출 요약 집계 중 DB 오류가 발생했습니다.") from exc

        return result

    def _get_sales_margin_snapshot(
        self,
        *,
        connection,
        store_id: str | None,
        target_date: str,
    ) -> dict | None:
        if not target_date or not store_id or not has_table(self.engine, "raw_production_extract"):
            return None

        if has_table(self.engine, "mart_sales_margin_daily"):
            snapshot_row = (
                connection.execute(
                    text(
                        """
                        SELECT avg_margin_rate, avg_net_profit_per_item, product_count
                        FROM mart_sales_margin_daily
                        WHERE store_id = :store_id
                          AND target_date = :target_date
                        """
                    ),
                    {"store_id": store_id, "target_date": target_date},
                )
                .mappings()
                .first()
            )
            if snapshot_row:
                return dict(snapshot_row)

        target_day = datetime.strptime(target_date, "%Y%m%d").date()
        window_start = (target_day - timedelta(days=27)).strftime("%Y%m%d")
        window_end = target_day.strftime("%Y%m%d")
        fallback_row = (
            connection.execute(
                text(
                    """
                    WITH sold_products AS (
                        SELECT DISTINCT COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '') AS item_nm
                        FROM raw_daily_store_item
                        WHERE masked_stor_cd = :store_id
                          AND sale_dt >= :window_start
                          AND sale_dt <= :window_end
                          AND COALESCE(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC), 0) > 0
                    ),
                    product_margin AS (
                        SELECT
                            COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '') AS item_nm,
                            AVG(
                                (CAST(p.sale_prc AS NUMERIC) - CAST(p.item_cost AS NUMERIC))
                                / NULLIF(CAST(p.sale_prc AS NUMERIC), 0)
                            ) AS margin_rate,
                            AVG(CAST(p.sale_prc AS NUMERIC) - CAST(p.item_cost AS NUMERIC)) AS net_profit_per_item
                        FROM raw_production_extract p
                        JOIN sold_products s
                          ON s.item_nm = COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '')
                        WHERE p.masked_stor_cd = :store_id
                          AND p.prod_dt >= :window_start
                          AND p.prod_dt <= :window_end
                          AND CAST(p.sale_prc AS NUMERIC) > 0
                          AND CAST(p.item_cost AS NUMERIC) > 0
                        GROUP BY COALESCE(NULLIF(TRIM(CAST(p.item_nm AS TEXT)), ''), '')
                    )
                    SELECT
                        AVG(margin_rate) AS avg_margin_rate,
                        AVG(net_profit_per_item) AS avg_net_profit_per_item,
                        COUNT(*) AS product_count
                    FROM product_margin
                    """
                ),
                {
                    "store_id": store_id,
                    "window_start": window_start,
                    "window_end": window_end,
                },
            )
            .mappings()
            .first()
        )
        return dict(fallback_row) if fallback_row else None

    def _get_avg_ticket_distribution(
        self,
        *,
        connection,
        store_id: str | None,
        target_date: str,
    ) -> list[float]:
        if not target_date or not store_id:
            return []

        target_day = datetime.strptime(target_date, "%Y%m%d").date()
        window_start = (target_day - timedelta(days=27)).strftime("%Y%m%d")

        analytics_daily_table = get_store_mart_table(store_id, "analytics", "daily_table")
        if analytics_daily_table and has_table(self.engine, analytics_daily_table):
            rows = (
                connection.execute(
                    text(
                        f"""
                        SELECT sale_dt, total_order_count
                        FROM {analytics_daily_table}
                        WHERE sale_dt >= :window_start
                          AND sale_dt <= :target_date
                          AND COALESCE(total_order_count, 0) > 0
                        ORDER BY sale_dt
                        """
                    ),
                    {"window_start": window_start, "target_date": target_date},
                )
                .mappings()
                .all()
            )
            if rows:
                sales_by_date = {
                    str(row["sale_dt"]): float(row["amount"] or 0)
                    for row in (
                        connection.execute(
                            text(
                                """
                                SELECT CAST(sale_dt AS TEXT) AS sale_dt,
                                       COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)), 0) AS amount
                                FROM raw_daily_store_item
                                WHERE masked_stor_cd = :store_id
                                  AND sale_dt >= :window_start
                                  AND sale_dt <= :target_date
                                GROUP BY sale_dt
                                """
                            ),
                            {
                                "store_id": store_id,
                                "window_start": window_start,
                                "target_date": target_date,
                            },
                        )
                        .mappings()
                        .all()
                    )
                }
                analytics_values = [
                    sales_by_date.get(str(row["sale_dt"]), 0.0) / float(row["total_order_count"])
                    for row in rows
                    if float(row["total_order_count"] or 0) > 0
                ]
                if analytics_values:
                    return analytics_values

        if has_table(self.engine, "core_channel_sales"):
            rows = (
                connection.execute(
                    text(
                        """
                        SELECT
                            sale_dt,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)), 0) AS revenue,
                            COALESCE(SUM(COALESCE(NULLIF(TRIM(CAST(ord_cnt AS TEXT)), '')::numeric, 0)), 0) AS order_count
                        FROM core_channel_sales
                        WHERE masked_stor_cd = :store_id
                          AND sale_dt >= :window_start
                          AND sale_dt <= :target_date
                        GROUP BY sale_dt
                        ORDER BY sale_dt
                        """
                    ),
                    {
                        "store_id": store_id,
                        "window_start": window_start,
                        "target_date": target_date,
                    },
                )
                .mappings()
                .all()
            )
            return [
                float(row["revenue"] or 0) / float(row["order_count"])
                for row in rows
                if float(row["order_count"] or 0) > 0
            ]

        return []

    @staticmethod
    def _normalize_index_from_distribution(
        *,
        current_value: float,
        values: list[float],
    ) -> float:
        valid_values = [float(value) for value in values if value is not None]
        if not valid_values or current_value <= 0:
            return 0.0
        min_value = min(valid_values)
        max_value = max(valid_values)
        if max_value <= min_value:
            return 100.0
        score = ((current_value - min_value) / (max_value - min_value)) * 100
        return max(0.0, min(100.0, score))

    async def get_dashboard_overview(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> dict[str, int | list[dict[str, int | str]]]:
        if not self.engine:
            raise RuntimeError("sales database engine is not configured")

        effective_reference = reference_datetime or get_now()
        target_date = (
            datetime.strptime(business_date, "%Y-%m-%d").date()
            if business_date
            else effective_reference.date()
        )
        target_dt = target_date.strftime("%Y%m%d")
        month_start = target_date.replace(day=1)
        last_month_end = month_start - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        target_hour = effective_reference.hour
        weekday_dates = self._build_recent_dates(target_date, 6)
        weekday_labels = self._build_weekday_labels(target_date, 6)
        month_points = self._build_month_points(target_date, 6)

        store_filter = ""
        item_store_filter = ""
        params: dict[str, str | int] = {
            "target_dt": target_dt,
            "month_start": month_start.strftime("%Y%m%d"),
            "last_month_start": last_month_start.strftime("%Y%m%d"),
            "last_month_end": last_month_end.strftime("%Y%m%d"),
            "target_hour": target_hour,
        }
        if store_id:
            store_filter = "AND masked_stor_cd = :store_id"
            item_store_filter = "AND t.masked_stor_cd = :store_id"
            params["store_id"] = store_id

        result = {
            "monthly_sales": 0,
            "today_sales": 0,
            "current_hour_sales": 0,
            "last_month_sales": 0,
            "last_month_same_weekday_avg_sales": 0,
            "last_month_same_hour_avg_sales": 0,
            "monthly_sales_points": [
                {"label": point["label"], "value": 0} for point in month_points
            ],
            "today_sales_points": [
                {"label": label, "value": 0} for label in weekday_labels
            ],
            "current_hour_sales_points": self._build_hour_points_range(
                start_hour=max(target_hour - 5, 0),
                target_hour=target_hour,
                rows=[],
            ),
        }

        try:
            with self.engine.connect() as connection:
                monthly_row = (
                    connection.execute(
                        text(
                            f"""
                            SELECT COALESCE(
                                SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                0
                            ) AS amount
                            FROM raw_daily_store_item
                            WHERE sale_dt >= :month_start AND sale_dt < :target_dt
                              {store_filter}
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
                monthly_amount_before_target = float(monthly_row["amount"] or 0) if monthly_row else 0.0

                today_row = (
                    connection.execute(
                        text(
                            f"""
                            SELECT COALESCE(
                                SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                0
                            ) AS amount
                            FROM raw_daily_store_item
                            WHERE sale_dt = :target_dt
                              {store_filter}
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
                full_day_today_amount = float(today_row["amount"] or 0) if today_row else 0.0

                last_month_row = (
                    connection.execute(
                        text(
                            f"""
                            SELECT COALESCE(
                                SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                0
                            ) AS amount
                            FROM raw_daily_store_item
                            WHERE sale_dt >= :last_month_start AND sale_dt <= :last_month_end
                              {store_filter}
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
                if last_month_row:
                    result["last_month_sales"] = int(round(float(last_month_row["amount"] or 0)))

                weekday_rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT CAST(sale_dt AS TEXT) AS sale_dt,
                                   COALESCE(
                                       SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                       0
                                   ) AS amount
                            FROM raw_daily_store_item
                            WHERE sale_dt >= :last_month_start AND sale_dt <= :last_month_end
                              {store_filter}
                            GROUP BY sale_dt
                            ORDER BY sale_dt
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
                weekday_values: list[float] = []
                for row in weekday_rows:
                    raw_date = str(row["sale_dt"])
                    try:
                        parsed = date_type(
                            int(raw_date[:4]),
                            int(raw_date[4:6]),
                            int(raw_date[6:8]),
                        )
                    except (TypeError, ValueError):
                        continue
                    if parsed.weekday() == target_date.weekday():
                        weekday_values.append(float(row["amount"] or 0))
                if weekday_values:
                    result["last_month_same_weekday_avg_sales"] = int(
                        round(sum(weekday_values) / len(weekday_values))
                    )

                if has_table(self.engine, "raw_daily_store_item_tmzon"):
                    today_cutoff_row = (
                        connection.execute(
                            text(
                                f"""
                                SELECT COALESCE(
                                    SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                    0
                                ) AS amount
                                FROM raw_daily_store_item_tmzon
                                WHERE sale_dt = :target_dt
                                  AND CAST(tmzon_div AS INTEGER) <= :target_hour
                                  {store_filter}
                                """
                            ),
                            params,
                        )
                        .mappings()
                        .first()
                    )
                    today_cutoff_amount = (
                        float(today_cutoff_row["amount"] or 0) if today_cutoff_row else 0.0
                    )
                    result["today_sales"] = int(round(today_cutoff_amount))
                    result["monthly_sales"] = int(round(monthly_amount_before_target + today_cutoff_amount))

                    current_hour_row = (
                        connection.execute(
                            text(
                                f"""
                                SELECT COALESCE(
                                    SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                    0
                                ) AS amount
                                FROM raw_daily_store_item_tmzon
                                WHERE sale_dt = :target_dt
                                  AND CAST(tmzon_div AS INTEGER) = :target_hour
                                  {store_filter}
                                """
                            ),
                            params,
                        )
                        .mappings()
                        .first()
                    )
                    if current_hour_row:
                        result["current_hour_sales"] = int(
                            round(float(current_hour_row["amount"] or 0))
                        )

                    last_hour_rows = (
                        connection.execute(
                            text(
                                f"""
                                SELECT sale_dt,
                                       COALESCE(
                                           AVG(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                           0
                                       ) AS amount
                                FROM raw_daily_store_item_tmzon t
                                WHERE sale_dt >= :last_month_start AND sale_dt <= :last_month_end
                                  AND CAST(tmzon_div AS INTEGER) = :target_hour
                                  {item_store_filter}
                                GROUP BY sale_dt
                                ORDER BY sale_dt
                                """
                            ),
                            params,
                        )
                        .mappings()
                        .all()
                    )
                    hour_values = [float(row["amount"] or 0) for row in last_hour_rows]
                    if hour_values:
                        result["last_month_same_hour_avg_sales"] = int(
                            round(sum(hour_values) / len(hour_values))
                        )

                    previous_dt = (target_date - timedelta(days=1)).strftime("%Y%m%d")
                    opening_row = (
                        connection.execute(
                            text(
                                f"""
                                SELECT MIN(CAST(tmzon_div AS INTEGER)) AS first_hour
                                FROM raw_daily_store_item_tmzon
                                WHERE sale_dt = :previous_dt
                                  AND CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC) > 0
                                  {store_filter}
                                """
                            ),
                            {**params, "previous_dt": previous_dt},
                        )
                        .mappings()
                        .first()
                    )
                    opening_hour = (
                        int(opening_row["first_hour"])
                        if opening_row and opening_row.get("first_hour") is not None
                        else max(target_hour - 5, 0)
                    )
                    if opening_hour > target_hour:
                        opening_hour = max(target_hour - 5, 0)

                    hour_rows = (
                        connection.execute(
                            text(
                                f"""
                                SELECT CAST(tmzon_div AS INTEGER) AS hour_div,
                                       COALESCE(
                                           SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                           0
                                       ) AS amount
                                FROM raw_daily_store_item_tmzon
                                WHERE sale_dt = :target_dt
                                  AND CAST(tmzon_div AS INTEGER) BETWEEN :hour_start AND :target_hour
                                  {store_filter}
                                GROUP BY hour_div
                                ORDER BY hour_div
                                """
                            ),
                            {
                                **params,
                                "hour_start": opening_hour,
                            },
                        )
                        .mappings()
                        .all()
                    )
                    result["current_hour_sales_points"] = self._build_hour_points_range(
                        start_hour=opening_hour,
                        target_hour=target_hour,
                        rows=hour_rows,
                    )
                else:
                    result["today_sales"] = int(round(full_day_today_amount))
                    result["monthly_sales"] = int(round(monthly_amount_before_target + full_day_today_amount))

                today_rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT CAST(sale_dt AS TEXT) AS sale_dt,
                                   COALESCE(
                                       SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                       0
                                   ) AS amount
                            FROM raw_daily_store_item
                            WHERE sale_dt >= :today_window_start AND sale_dt <= :target_dt
                              {store_filter}
                            GROUP BY sale_dt
                            ORDER BY sale_dt
                            """
                        ),
                        {
                            **params,
                            "today_window_start": weekday_dates[0],
                        },
                    )
                    .mappings()
                    .all()
                )
                today_points = self._build_points(
                    labels=weekday_labels,
                    keys=weekday_dates,
                    rows=today_rows,
                    row_key="sale_dt",
                )
                if today_points:
                    today_points[-1]["value"] = result["today_sales"]
                result["today_sales_points"] = today_points

                month_rows = (
                    connection.execute(
                        text(
                            f"""
                            SELECT SUBSTRING(CAST(sale_dt AS TEXT), 1, 6) AS month_key,
                                   COALESCE(
                                       SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                       0
                                   ) AS amount
                            FROM raw_daily_store_item
                            WHERE sale_dt >= :month_window_start AND sale_dt <= :target_dt
                              {store_filter}
                            GROUP BY month_key
                            ORDER BY month_key
                            """
                        ),
                        {
                            **params,
                            "month_window_start": month_points[0]["key"] + "01",
                        },
                    )
                    .mappings()
                    .all()
                )
                monthly_points_result = self._build_points(
                    labels=[point["label"] for point in month_points],
                    keys=[point["key"] for point in month_points],
                    rows=month_rows,
                    row_key="month_key",
                )
                if monthly_points_result:
                    monthly_points_result[-1]["value"] = result["monthly_sales"]
                result["monthly_sales_points"] = monthly_points_result
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to aggregate dashboard sales overview (store_id=%s, business_date=%s): %s",
                store_id,
                business_date,
                exc,
            )
            raise RuntimeError("대시보드 매출 집계 중 DB 오류가 발생했습니다.") from exc

        return result

    @staticmethod
    def _build_recent_dates(target_date: date_type, points: int) -> list[str]:
        return [
            (target_date - timedelta(days=points - index - 1)).strftime("%Y%m%d")
            for index in range(points)
        ]

    @staticmethod
    def _build_weekday_labels(target_date: date_type, points: int) -> list[str]:
        labels = ["월", "화", "수", "목", "금", "토", "일"]
        return [
            labels[(target_date - timedelta(days=points - index - 1)).weekday()]
            for index in range(points)
        ]

    @staticmethod
    def _build_month_points(target_date: date_type, points: int) -> list[dict[str, str]]:
        month_points: list[dict[str, str]] = []
        month_cursor = target_date.replace(day=1)
        month_starts: list[date_type] = []
        for _ in range(points):
            month_starts.append(month_cursor)
            month_cursor = (month_cursor - timedelta(days=1)).replace(day=1)

        for month_start in reversed(month_starts):
            month_points.append(
                {
                    "key": month_start.strftime("%Y%m"),
                    "label": f"{month_start.year % 100:02d}.{month_start.month:02d}",
                }
            )
        return month_points

    @staticmethod
    def _build_points(
        labels: list[str],
        keys: list[str],
        rows: list[dict],
        row_key: str,
    ) -> list[dict[str, int | str]]:
        values_by_key = {
            str(row.get(row_key)): int(round(float(row.get("amount") or 0)))
            for row in rows
        }
        return [
            {"label": label, "value": values_by_key.get(key, 0)}
            for label, key in zip(labels, keys, strict=False)
        ]

    @staticmethod
    def _build_hour_points(
        target_hour: int,
        rows: list[dict],
    ) -> list[dict[str, int | str]]:
        values_by_hour = {
            int(row.get("hour_div")): int(round(float(row.get("amount") or 0)))
            for row in rows
            if row.get("hour_div") is not None
        }
        return [
            {
                "label": f"{(target_hour - 5 + index) % 24}시",
                "value": values_by_hour.get((target_hour - 5 + index) % 24, 0),
            }
            for index in range(6)
        ]

    @staticmethod
    def _build_hour_points_range(
        start_hour: int,
        target_hour: int,
        rows: list[dict],
    ) -> list[dict[str, int | str]]:
        values_by_hour = {
            int(row.get("hour_div")): int(round(float(row.get("amount") or 0)))
            for row in rows
            if row.get("hour_div") is not None
        }
        if start_hour > target_hour:
            start_hour = max(target_hour - 5, 0)
        return [
            {
                "label": f"{hour}시",
                "value": values_by_hour.get(hour, 0),
            }
            for hour in range(start_hour, target_hour + 1)
        ]
