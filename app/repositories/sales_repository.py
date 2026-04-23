from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date as date_type
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

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
            "avg_margin_rate": 0.0,
            "avg_net_profit_per_item": 0.0,
            "estimated_today_profit": 0.0,
        }

        # 1. 사용할 테이블·컬럼 결정
        item_relation = "raw_daily_store_item"
        amt_col = "sale_amt"
        net_col = "net_sale_amt"
        qty_col = "sale_qty"

        store_filter = ""
        params: dict = {}
        if store_id:
            store_filter = "AND masked_stor_cd = :store_id"
            params["store_id"] = store_id
        if date_from:
            store_filter += " AND sale_dt >= :date_from"
            params["date_from"] = date_from.replace("-", "")
        if date_to:
            store_filter += " AND sale_dt <= :date_to"
            params["date_to"] = date_to.replace("-", "")

        try:
            with self.engine.connect() as connection:
                # 2. 최신 데이터 날짜 조회
                max_dt_row = (
                    connection.execute(
                        text(
                            f"SELECT MAX(sale_dt) AS max_dt FROM {item_relation} WHERE sale_dt IS NOT NULL {store_filter}"
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
                if not max_dt_row or not max_dt_row["max_dt"]:
                    raise LookupError("매출 요약 데이터가 없습니다.")
                max_dt = str(max_dt_row["max_dt"])
                result["data_date"] = max_dt

                # 3. 오늘(최신일) 매출 집계
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

                # 4. 최근 7개 영업일 데이터 집계
                weekly_rows = (
                    connection.execute(
                        text(
                            f"""
                        WITH recent_dates AS (
                            SELECT DISTINCT sale_dt
                            FROM {item_relation}
                            WHERE sale_dt IS NOT NULL {store_filter}
                            ORDER BY sale_dt DESC
                            LIMIT 7
                        )
                        SELECT
                            d.sale_dt,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(t.{amt_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS revenue,
                            COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(t.{net_col} AS TEXT), ''), '0') AS NUMERIC)), 0) AS net_revenue
                        FROM recent_dates d
                        JOIN {item_relation} t ON t.sale_dt = d.sale_dt {store_filter.replace("AND ", "AND t.")}
                        GROUP BY d.sale_dt
                        ORDER BY d.sale_dt ASC
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )

                _DAY_LABELS = ["월", "화", "수", "목", "금", "토", "일"]
                weekly = []
                for row in weekly_rows:
                    dt_str = str(row["sale_dt"])
                    if len(dt_str) == 8:
                        try:
                            from datetime import datetime as _dt

                            dow = _dt.strptime(dt_str, "%Y%m%d").weekday()
                            day_label = _DAY_LABELS[dow]
                        except ValueError:
                            day_label = dt_str[-2:]
                    else:
                        day_label = dt_str
                    weekly.append(
                        {
                            "day": day_label,
                            "revenue": float(row["revenue"] or 0),
                            "net_revenue": float(row["net_revenue"] or 0),
                        }
                    )
                result["weekly_data"] = weekly

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
                        LIMIT 5
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
                if has_table(self.engine, "raw_production_extract"):
                    cost_filter = ""
                    cost_params: dict = {}
                    if store_id:
                        cost_filter = "AND masked_stor_cd = :store_id"
                        cost_params["store_id"] = store_id
                    cost_row = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                AVG(
                                    (CAST(sale_prc AS NUMERIC) - CAST(item_cost AS NUMERIC))
                                    / NULLIF(CAST(sale_prc AS NUMERIC), 0)
                                ) AS avg_margin_rate,
                                AVG(
                                    CAST(sale_prc AS NUMERIC) - CAST(item_cost AS NUMERIC)
                                ) AS avg_net_profit_per_item
                            FROM raw_production_extract
                            WHERE CAST(sale_prc AS NUMERIC) > 0
                              AND CAST(item_cost AS NUMERIC) > 0
                              {cost_filter}
                            """
                            ),
                            cost_params,
                        )
                        .mappings()
                        .first()
                    )
                    if cost_row and cost_row["avg_margin_rate"] is not None:
                        avg_margin = float(cost_row["avg_margin_rate"] or 0)
                        avg_net = float(cost_row["avg_net_profit_per_item"] or 0)
                        result["avg_margin_rate"] = round(avg_margin, 4)
                        result["avg_net_profit_per_item"] = round(avg_net, 2)
                        result["estimated_today_profit"] = round(
                            result["today_revenue"] * avg_margin, 2
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

    async def get_dashboard_overview(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
    ) -> dict[str, int]:
        if not self.engine:
            raise RuntimeError("sales database engine is not configured")

        target_date = (
            datetime.strptime(business_date, "%Y-%m-%d").date()
            if business_date
            else datetime.utcnow().date()
        )
        target_dt = target_date.strftime("%Y%m%d")
        month_start = target_date.replace(day=1)
        last_month_end = month_start - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)

        store_filter = ""
        item_store_filter = ""
        params: dict[str, str | int] = {
            "target_dt": target_dt,
            "month_start": month_start.strftime("%Y%m%d"),
            "last_month_start": last_month_start.strftime("%Y%m%d"),
            "last_month_end": last_month_end.strftime("%Y%m%d"),
            "target_hour": datetime.now().hour,
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
                            WHERE sale_dt >= :month_start AND sale_dt <= :target_dt
                              {store_filter}
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
                if monthly_row:
                    result["monthly_sales"] = int(round(float(monthly_row["amount"] or 0)))

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
                if today_row:
                    result["today_sales"] = int(round(float(today_row["amount"] or 0)))

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
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to aggregate dashboard sales overview (store_id=%s, business_date=%s): %s",
                store_id,
                business_date,
                exc,
            )
            raise RuntimeError("대시보드 매출 집계 중 DB 오류가 발생했습니다.") from exc

        return result
