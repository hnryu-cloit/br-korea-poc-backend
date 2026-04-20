from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table


class HQRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def list_coaching_rows(self, limit: int = 5) -> list[dict]:
        return self._fetch_coaching_rows(limit=limit)

    async def list_inspection_rows(self, limit: int = 5) -> list[dict]:
        return self._fetch_inspection_rows(limit=limit)

    def _fetch_coaching_rows(self, limit: int = 5) -> list[dict]:
        if not self.engine or not (
            has_table(self.engine, "ordering_selections")
            and has_table(self.engine, "core_store_master")
        ):
            return []

        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            """
                        WITH latest_orders AS (
                            SELECT DISTINCT ON (NULLIF(TRIM(CAST(store_id AS TEXT)), ''))
                                NULLIF(TRIM(CAST(store_id AS TEXT)), '') AS store_id,
                                option_id,
                                reason,
                                actor,
                                TO_CHAR(selected_at, 'YYYY-MM-DD HH24:MI') AS submitted_at,
                                selected_at
                            FROM ordering_selections
                            WHERE NULLIF(TRIM(CAST(store_id AS TEXT)), '') IS NOT NULL
                            ORDER BY NULLIF(TRIM(CAST(store_id AS TEXT)), ''), selected_at DESC
                        ),
                        order_counts AS (
                            SELECT
                                NULLIF(TRIM(CAST(store_id AS TEXT)), '') AS store_id,
                                COUNT(*) AS order_count_7d,
                                COUNT(*) FILTER (WHERE option_id = 'opt-a') AS recommended_count_7d,
                                MAX(selected_at) AS last_order_at
                            FROM ordering_selections
                            WHERE selected_at >= NOW() - INTERVAL '7 days'
                              AND NULLIF(TRIM(CAST(store_id AS TEXT)), '') IS NOT NULL
                            GROUP BY NULLIF(TRIM(CAST(store_id AS TEXT)), '')
                        ),
                        production_counts AS (
                            SELECT
                                NULLIF(TRIM(CAST(store_id AS TEXT)), '') AS store_id,
                                COUNT(*) AS production_count_7d,
                                COALESCE(SUM(qty), 0) AS production_qty_7d,
                                MAX(registered_at) AS last_production_at
                            FROM production_registrations
                            WHERE registered_at >= NOW() - INTERVAL '7 days'
                              AND NULLIF(TRIM(CAST(store_id AS TEXT)), '') IS NOT NULL
                            GROUP BY NULLIF(TRIM(CAST(store_id AS TEXT)), '')
                        )
                        SELECT
                            NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '') AS store_id,
                            COALESCE(NULLIF(TRIM(sm.masked_stor_nm), ''), NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), ''), '미지정 매장') AS store,
                            COALESCE(NULLIF(TRIM(sm.region), ''), '전체') AS region,
                            lo.option_id,
                            lo.reason,
                            lo.submitted_at,
                            COALESCE(oc.order_count_7d, 0) AS order_count_7d,
                            COALESCE(oc.recommended_count_7d, 0) AS recommended_count_7d,
                            COALESCE(pc.production_count_7d, 0) AS production_count_7d,
                            COALESCE(pc.production_qty_7d, 0) AS production_qty_7d,
                            COALESCE(sm.actual_sales_amt, 0) AS actual_sales_amt,
                            COALESCE(sm.campaign_sales_ratio, 0) AS campaign_sales_ratio
                        FROM core_store_master sm
                        LEFT JOIN latest_orders lo
                          ON lo.store_id = NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '')
                        LEFT JOIN order_counts oc
                          ON oc.store_id = NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '')
                        LEFT JOIN production_counts pc
                          ON pc.store_id = NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '')
                        WHERE lo.store_id IS NOT NULL
                           OR oc.store_id IS NOT NULL
                           OR pc.store_id IS NOT NULL
                        ORDER BY
                            COALESCE(pc.production_qty_7d, 0) + COALESCE(oc.order_count_7d, 0) DESC,
                            COALESCE(sm.actual_sales_amt, 0) DESC,
                            store
                        LIMIT :limit
                        """
                        ),
                        {"limit": limit},
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return []

        return [dict(row) for row in rows]

    def _fetch_inspection_rows(self, limit: int = 5) -> list[dict]:
        if not self.engine or not (
            has_table(self.engine, "core_store_master")
            and has_table(self.engine, "ordering_selections")
            and has_table(self.engine, "production_registrations")
        ):
            return []

        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            """
                        WITH order_7d AS (
                            SELECT
                                NULLIF(TRIM(CAST(store_id AS TEXT)), '') AS store_id,
                                COUNT(*) AS order_count_7d,
                                COUNT(*) FILTER (WHERE option_id = 'opt-a') AS recommended_count_7d,
                                MAX(selected_at) AS last_order_at
                            FROM ordering_selections
                            WHERE selected_at >= NOW() - INTERVAL '7 days'
                              AND NULLIF(TRIM(CAST(store_id AS TEXT)), '') IS NOT NULL
                            GROUP BY NULLIF(TRIM(CAST(store_id AS TEXT)), '')
                        ),
                        production_7d AS (
                            SELECT
                                NULLIF(TRIM(CAST(store_id AS TEXT)), '') AS store_id,
                                COUNT(*) AS production_count_7d,
                                COALESCE(SUM(qty), 0) AS production_qty_7d,
                                MAX(registered_at) AS last_production_at
                            FROM production_registrations
                            WHERE registered_at >= NOW() - INTERVAL '7 days'
                              AND NULLIF(TRIM(CAST(store_id AS TEXT)), '') IS NOT NULL
                            GROUP BY NULLIF(TRIM(CAST(store_id AS TEXT)), '')
                        ),
                        production_prev_7d AS (
                            SELECT
                                NULLIF(TRIM(CAST(store_id AS TEXT)), '') AS store_id,
                                COALESCE(SUM(qty), 0) AS production_qty_prev_7d
                            FROM production_registrations
                            WHERE registered_at >= NOW() - INTERVAL '14 days'
                              AND registered_at < NOW() - INTERVAL '7 days'
                              AND NULLIF(TRIM(CAST(store_id AS TEXT)), '') IS NOT NULL
                            GROUP BY NULLIF(TRIM(CAST(store_id AS TEXT)), '')
                        )
                        SELECT
                            NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '') AS store_id,
                            COALESCE(NULLIF(TRIM(sm.masked_stor_nm), ''), NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), ''), '미지정 매장') AS store,
                            COALESCE(NULLIF(TRIM(sm.region), ''), '전체') AS region,
                            COALESCE(oa.order_count_7d, 0) AS order_count_7d,
                            COALESCE(oa.recommended_count_7d, 0) AS recommended_count_7d,
                            COALESCE(oa.last_order_at, NOW() - INTERVAL '30 days') AS last_order_at,
                            COALESCE(pa.production_count_7d, 0) AS production_count_7d,
                            COALESCE(pa.production_qty_7d, 0) AS production_qty_7d,
                            COALESCE(pa.last_production_at, NOW() - INTERVAL '30 days') AS last_production_at,
                            COALESCE(pp.production_qty_prev_7d, 0) AS production_qty_prev_7d,
                            COALESCE(sm.actual_sales_amt, 0) AS actual_sales_amt,
                            COALESCE(sm.campaign_sales_ratio, 0) AS campaign_sales_ratio,
                            COALESCE(sm.store_area_pyeong, 0) AS store_area_pyeong
                        FROM core_store_master sm
                        LEFT JOIN order_7d oa
                          ON oa.store_id = NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '')
                        LEFT JOIN production_7d pa
                          ON pa.store_id = NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '')
                        LEFT JOIN production_prev_7d pp
                          ON pp.store_id = NULLIF(TRIM(CAST(sm.masked_stor_cd AS TEXT)), '')
                        WHERE oa.store_id IS NOT NULL
                           OR pa.store_id IS NOT NULL
                        ORDER BY
                            COALESCE(pa.production_qty_7d, 0) + COALESCE(oa.order_count_7d, 0) DESC,
                            COALESCE(sm.actual_sales_amt, 0) DESC,
                            store
                        LIMIT :limit
                        """
                        ),
                        {"limit": limit},
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return []

        return [dict(row) for row in rows]

    @staticmethod
    def _format_percentage_delta(current: float, previous: float) -> str:
        if previous <= 0:
            if current <= 0:
                return "0%"
            return "+100%"
        delta = round(((current - previous) / previous) * 100)
        sign = "+" if delta > 0 else ""
        return f"{sign}{delta}%"
