from __future__ import annotations

import logging

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
