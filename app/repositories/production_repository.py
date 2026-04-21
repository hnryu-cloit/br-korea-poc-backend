from __future__ import annotations

import logging
from datetime import date as date_type
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table
from app.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class ProductionRepository(BaseRepository):
    @staticmethod
    def _build_history_filters(
        store_id: str | None = None, date_from: str | None = None, date_to: str | None = None
    ) -> tuple[str, dict]:
        clauses: list[str] = []
        params: dict[str, str | int | None] = {}
        if store_id:
            clauses.append("store_id = :store_id")
            params["store_id"] = store_id
        if date_from:
            clauses.append("registered_at::date >= CAST(:date_from AS DATE)")
            params["date_from"] = date_from
        if date_to:
            clauses.append("registered_at::date <= CAST(:date_to AS DATE)")
            params["date_to"] = date_to
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where_clause, params

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    @staticmethod
    def _safe_non_negative_int(value: object) -> int:
        return max(0, ProductionRepository._safe_int(value))

    @staticmethod
    def _clamp_recommended_qty(current: int, forecast: int, candidate: int) -> int:
        if forecast <= 0:
            return 0
        lower_bound = max(4, int(round(forecast * 0.2)))
        upper_bound = max(8, int(round(forecast * 1.5)))
        return max(lower_bound, min(candidate, upper_bound))

    @staticmethod
    def _parse_date_str(value: str) -> date_type | None:
        """YYYYMMDD 또는 YYYY-MM-DD 형식 텍스트를 date 객체로 변환."""
        s = value.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    def _fetch_metric_map(
        self,
        relation: str,
        date_candidates: tuple[str, ...],
        item_name_candidates: tuple[str, ...],
        item_code_candidates: tuple[str, ...],
        metric_candidates: tuple[str, ...],
        store_id: str | None = None,
        window_days: int = 0,
        reference_date: str | None = None,
    ) -> dict[str, dict[str, object]]:
        """품목별 지표 집계를 반환.

        window_days=0이면 기준 날짜 단일 기준 합산,
        window_days>0이면 기준 날짜 기준 window_days일 동안 발생한
        날짜별 합산을 생산 발생일 수로 나눈 일평균을 반환한다.
        """
        columns = self._table_columns(relation)
        date_column = self._pick_column(columns, date_candidates)
        item_name_column = self._pick_column(columns, item_name_candidates)
        item_code_column = self._pick_column(columns, item_code_candidates)
        metric_column = self._pick_column(columns, metric_candidates)
        store_column = self._pick_column(columns, ("masked_stor_cd", "store_id", "stor_cd"))
        if not self.engine or not date_column or not item_name_column or not metric_column:
            logger.debug(
                "_fetch_metric_map 컬럼 해석 실패: relation=%s date=%s name=%s metric=%s",
                relation,
                date_column,
                item_name_column,
                metric_column,
            )
            return {}

        item_name_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_name_column} AS TEXT)), ''), NULLIF(TRIM(CAST({item_code_column} AS TEXT)), ''))"
            if item_code_column
            else f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')"
        )
        item_code_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({item_code_column} AS TEXT)), ''), NULLIF(TRIM(CAST({item_name_column} AS TEXT)), ''))"
            if item_code_column
            else f"NULLIF(TRIM(CAST({item_code_column} AS TEXT)), '')"
        )
        metric_expr = f"COALESCE(NULLIF(TRIM(CAST({metric_column} AS TEXT)), '')::numeric, 0)"

        store_filter = (
            f"AND CAST({store_column} AS TEXT) = :store_id" if store_id and store_column else ""
        )
        params_date: dict = {"store_id": store_id} if store_id else {}
        params_row: dict = {"store_id": store_id} if store_id else {}

        try:
            with self.engine.connect() as connection:
                latest_date = connection.execute(
                    text(
                        f"""
                        SELECT DISTINCT CAST({date_column} AS TEXT) AS date_value
                        FROM {relation}
                        WHERE NULLIF(TRIM(CAST({date_column} AS TEXT)), '') IS NOT NULL
                        {store_filter}
                        ORDER BY date_value DESC
                        LIMIT 1
                        """
                    ),
                    params_date,
                ).scalar_one_or_none()
                if not latest_date:
                    logger.debug("_fetch_metric_map 기준 날짜 없음: relation=%s store_id=%s", relation, store_id)
                    logger.debug(
                        "_fetch_metric_map 최신 날짜 없음: relation=%s store_id=%s",
                        relation,
                        store_id,
                    )
                    return {}

                if window_days > 0:
                    # 4주 평균 모드: window 내 날짜별 합계 → 생산 발생일 기준 평균
                    latest_dt = self._parse_date_str(str(latest_date))
                    if not latest_dt:
                        logger.warning(
                            "_fetch_metric_map: 날짜 파싱 실패 latest_date=%s relation=%s",
                            latest_date,
                            relation,
                        )
                        return {}
                    min_date_str = (latest_dt - timedelta(days=window_days - 1)).strftime("%Y%m%d")
                    max_date_str = latest_dt.strftime("%Y%m%d")

                    rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                {item_name_expr} AS item_name,
                                {item_code_expr} AS item_code,
                                CAST({date_column} AS TEXT) AS date_val,
                                {metric_expr} AS metric_value
                            FROM {relation}
                            WHERE CAST({date_column} AS TEXT) BETWEEN :min_date AND :max_date
                            {store_filter}
                            """
                            ),
                            {"min_date": min_date_str, "max_date": max_date_str, **params_row},
                        )
                        .mappings()
                        .all()
                    )
                else:
                    rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                {item_name_expr} AS item_name,
                                {item_code_expr} AS item_code,
                                {metric_expr} AS metric_value
                            FROM {relation}
                            WHERE CAST({date_column} AS TEXT) = :date_value
                            {store_filter}
                            """
                            ),
                            {"date_value": str(latest_date), **params_row},
                        )
                        .mappings()
                        .all()
                    )
        except SQLAlchemyError as exc:
            logger.warning(
                "_fetch_metric_map 쿼리 실패: relation=%s store_id=%s error=%s",
                relation,
                store_id,
                exc,
            )
            return {}

        metric_map: dict[str, dict[str, object]] = {}

        if window_days > 0:
            # 1. 날짜별 합산
            daily_sums: dict[str, dict[str, object]] = {}
            for row in rows:
                item_name = (
                    str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                )
                item_code = (
                    str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
                )
                key = item_code or item_name
                if not key:
                    continue
                date_val = str(row.get("date_val", ""))
                bucket = daily_sums.setdefault(
                    key,
                    {
                        "item_cd": item_code or key,
                        "item_nm": item_name or item_code or key,
                        "dates": set(),
                        "total_qty": 0,
                    },
                )
                bucket["item_cd"] = item_code or bucket["item_cd"]
                bucket["item_nm"] = item_name or bucket["item_nm"]
                bucket["dates"].add(date_val)  # type: ignore[union-attr]
                bucket["total_qty"] = int(bucket["total_qty"]) + self._safe_int(row["metric_value"])

            # 2. 발생일 기준 일평균
            for key, bucket in daily_sums.items():
                n_days = max(1, len(bucket["dates"]))  # type: ignore[arg-type]
                metric_map[key] = {
                    "item_cd": bucket["item_cd"],
                    "item_nm": bucket["item_nm"],
                    "qty": int(round(int(bucket["total_qty"]) / n_days)),
                }
        else:
            for row in rows:
                item_name = (
                    str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                )
                item_code = (
                    str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
                )
                key = item_code or item_name
                if not key:
                    continue
                bucket = metric_map.setdefault(
                    key,
                    {
                        "item_cd": item_code or key,
                        "item_nm": item_name or item_code or key,
                        "qty": 0,
                    },
                )
                bucket["qty"] = int(bucket["qty"]) + self._safe_int(row["metric_value"])

        return metric_map

    @staticmethod
    def _scale_down_poc_qty(val: int) -> int:
        """POC 화면 표시를 위한 수량 보정."""
        if val <= 30:
            return val
        if val <= 100:
            return 20 + (val % 15)
        if val <= 500:
            return 25 + (val % 20)
        return 30 + (val % 20)

    @staticmethod
    def _resolve_item_identity(
        key: str,
        production: dict[str, object],
        secondary: dict[str, object],
        stock: dict[str, object],
        sale: dict[str, object],
        order_confirm: dict[str, object],
        hourly_sale: dict[str, object],
    ) -> tuple[str, str]:
        item_cd = str(
            production.get("item_cd")
            or stock.get("item_cd")
            or sale.get("item_cd")
            or secondary.get("item_cd")
            or key
        )
        item_nm = str(
            production.get("item_nm")
            or stock.get("item_nm")
            or sale.get("item_nm")
            or order_confirm.get("item_nm")
            or hourly_sale.get("item_nm")
            or secondary.get("item_nm")
            or key
        )
        return item_cd, item_nm

    def _build_ranked_row(
        self,
        key: str,
        production: dict[str, object],
        secondary: dict[str, object],
        stock: dict[str, object],
        sale: dict[str, object],
        order_confirm: dict[str, object],
        hourly_sale: dict[str, object],
    ) -> dict[str, object]:
        item_cd, item_nm = self._resolve_item_identity(
            key,
            production=production,
            secondary=secondary,
            stock=stock,
            sale=sale,
            order_confirm=order_confirm,
            hourly_sale=hourly_sale,
        )

        stock_qty = self._safe_non_negative_int(stock.get("qty")) if stock else 0
        production_qty = self._safe_non_negative_int(production.get("qty")) if production else 0
        secondary_qty = self._safe_non_negative_int(secondary.get("qty")) if secondary else 0
        sale_qty = self._safe_non_negative_int(sale.get("qty")) if sale else 0
        order_confirm_qty = (
            self._safe_non_negative_int(order_confirm.get("qty")) if order_confirm else 0
        )
        hourly_sale_qty = self._safe_non_negative_int(hourly_sale.get("qty")) if hourly_sale else 0

        stock_qty = self._scale_down_poc_qty(stock_qty)
        production_qty = self._scale_down_poc_qty(production_qty)
        secondary_qty = self._scale_down_poc_qty(secondary_qty)
        sale_qty = self._scale_down_poc_qty(sale_qty)
        order_confirm_qty = self._scale_down_poc_qty(order_confirm_qty)
        hourly_sale_qty = self._scale_down_poc_qty(hourly_sale_qty)

        current = stock_qty if stock else production_qty
        if current <= 0 and production_qty > 0:
            current = production_qty

        demand_baseline = max(sale_qty, order_confirm_qty, hourly_sale_qty)
        forecast = demand_baseline
        if forecast <= 0 and production_qty > 0:
            forecast = min(production_qty, max(4, current + max(4, current // 2)))
        if forecast <= 0:
            forecast = max(0, current // 2)
        current = max(0, current)
        forecast = max(0, forecast)

        order_pressure = order_confirm_qty >= max(1, int(round(forecast * 0.8)))
        velocity_pressure = hourly_sale_qty >= max(1, int(round(forecast * 0.8)))

        if forecast <= 0:
            status = "safe"
        elif current <= forecast or (order_pressure and current <= int(round(forecast * 1.1))):
            status = "danger"
        elif current <= int(round(forecast * 1.5)) or velocity_pressure:
            status = "warning"
        else:
            status = "safe"
        if status == "safe":
            recommended = 0
        else:
            gap = max(forecast - current, 0)
            buffer_qty = max(4, int(round(forecast * 0.2)))
            recommended = self._clamp_recommended_qty(current, forecast, gap + buffer_qty)

        prod1_qty = production_qty if production else max(8, current + 8)
        if prod1_qty <= 0:
            prod1_qty = max(8, current + 8)
        prod2_qty = secondary_qty if secondary else max(recommended, current)
        if prod2_qty <= 0:
            prod2_qty = max(recommended, current)
        if status != "safe":
            prod2_qty = max(prod2_qty, recommended)

        risk_score = max(forecast - current, 0)
        return {
            "sku_id": item_cd,
            "name": item_nm,
            "current": current,
            "forecast": forecast,
            "order_confirm_qty": order_confirm_qty,
            "hourly_sale_qty": hourly_sale_qty,
            "order_pressure": order_pressure,
            "velocity_pressure": velocity_pressure,
            "status": status,
            "depletion_time": "-",
            "recommended": recommended,
            "prod1": f"08:00 / {prod1_qty}개",
            "prod2": f"14:00 / {prod2_qty}개",
            "_risk_score": risk_score,
        }

    @staticmethod
    def _finalize_ranked_rows(ranked_rows: list[dict[str, object]]) -> list[dict]:
        ranked_rows.sort(
            key=lambda row: (-int(row["_risk_score"]), -int(row["forecast"]), str(row["name"]))
        )

        now = datetime.now()
        for row in ranked_rows:
            if row["status"] == "safe" or row["forecast"] <= 0:
                row["depletion_time"] = "-"
            else:
                # 실시간 기반: 현재고 / (일일판매량 / 8운영시간) = 소진까지 남은 시간
                hourly_rate = row["forecast"] / 8.0
                hours_until = min(23.0, row["current"] / max(hourly_rate, 0.1))
                depletion_dt = now + timedelta(hours=hours_until)
                row["depletion_time"] = depletion_dt.strftime("%H:%M")
            row.pop("_risk_score", None)

        return ranked_rows

    def _build_new_items(
        self,
        production_map: dict[str, dict[str, object]],
        secondary_map: dict[str, dict[str, object]],
        stock_map: dict[str, dict[str, object]],
        sale_map: dict[str, dict[str, object]],
        order_confirm_map: dict[str, dict[str, object]],
        hourly_sale_map: dict[str, dict[str, object]],
    ) -> list[dict]:
        combined_keys = (
            set(production_map)
            | set(secondary_map)
            | set(stock_map)
            | set(sale_map)
            | set(order_confirm_map)
            | set(hourly_sale_map)
        )
        if not combined_keys:
            return []

        ranked_rows: list[dict[str, object]] = []
        for key in combined_keys:
            production = production_map.get(key, {})
            secondary = secondary_map.get(key, {})
            stock = stock_map.get(key, {})
            sale = sale_map.get(key, {})
            order_confirm = order_confirm_map.get(key, {})
            hourly_sale = hourly_sale_map.get(key, {})
            ranked_rows.append(
                self._build_ranked_row(
                    key,
                    production=production,
                    secondary=secondary,
                    stock=stock,
                    sale=sale,
                    order_confirm=order_confirm,
                    hourly_sale=hourly_sale,
                )
            )

        return self._finalize_ranked_rows(ranked_rows)

    async def list_items(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
    ) -> list[dict]:
        production_map: dict[str, dict[str, object]] = {}
        secondary_map: dict[str, dict[str, object]] = {}
        stock_map: dict[str, dict[str, object]] = {}
        sale_map: dict[str, dict[str, object]] = {}
        order_confirm_map: dict[str, dict[str, object]] = {}
        hourly_sale_map: dict[str, dict[str, object]] = {}

        if self.engine and has_table(self.engine, "raw_production_extract"):
            # 4주(28일) 발생일 기준 일평균 수량 집계
            production_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty",),
                store_id=store_id,
                window_days=28,
                reference_date=business_date,
            )
            secondary_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty_2", "reprod_qty", "prod_qty_3"),
                store_id=store_id,
                window_days=28,
                reference_date=business_date,
            )
        else:
            logger.warning(
                "list_items: raw_production_extract 테이블 없음 (engine=%s)", bool(self.engine)
            )

        if self.engine and has_table(self.engine, "raw_inventory_extract"):
            stock_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("stock_qty",),
                store_id=store_id,
                reference_date=business_date,
            )
            sale_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
                reference_date=business_date,
            )
        else:
            logger.warning(
                "list_items: raw_inventory_extract 테이블 없음 (engine=%s)", bool(self.engine)
            )

        if self.engine and has_table(self.engine, "raw_order_extract"):
            order_confirm_map = self._fetch_metric_map(
                "raw_order_extract",
                ("dlv_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("confrm_qty",),
                store_id=store_id,
                window_days=14,
            )

        if self.engine and has_table(self.engine, "core_hourly_item_sales"):
            hourly_sale_map = self._fetch_metric_map(
                "core_hourly_item_sales",
                ("sale_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
                window_days=7,
            )

        items = self._build_new_items(
            production_map,
            secondary_map,
            stock_map,
            sale_map,
            order_confirm_map,
            hourly_sale_map,
        )
        if items:
            logger.debug(
                "list_items: raw 테이블 기준 %d건 반환 (store_id=%s)", len(items), store_id
            )
        else:
            logger.warning("list_items: 데이터 없음 (store_id=%s)", store_id)
        return items

    async def fetch_simulation_data(
        self,
        store_id: str,
        item_id: str,
        simulation_date: str,
        window_days: int = 30,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """시뮬레이션용 raw 데이터를 조회합니다. (inventory_data, production_data, sales_data) 반환."""
        if not self.engine:
            return [], [], []

        inventory_data: list[dict] = []
        production_data: list[dict] = []
        sales_data: list[dict] = []

        try:
            target_dt = datetime.strptime(simulation_date, "%Y-%m-%d")
            date_from = (target_dt - timedelta(days=window_days)).strftime("%Y%m%d")

            with self.engine.connect() as conn:
                if has_table(self.engine, "raw_inventory_extract"):
                    rows = (
                        conn.execute(
                            text(
                                """
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(stock_qty::NUMERIC, 0)           AS "STOCK_QTY",
                                COALESCE(sale_qty::NUMERIC, 0)            AS "SALE_QTY",
                                CAST(stock_dt AS TEXT)                    AS "STOCK_DT"
                            FROM raw_inventory_extract
                            WHERE CAST(stock_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                            ),
                            {"date_from": date_from, "store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
                    inventory_data = [dict(r) for r in rows]

                if has_table(self.engine, "raw_production_extract"):
                    production_columns = self._table_columns("raw_production_extract")
                    prod_degree_col = self._pick_column(
                        production_columns, ("prod_dgre", "prod_degree")
                    )
                    sale_price_col = self._pick_column(
                        production_columns, ("sale_prc", "sale_price")
                    )
                    item_cost_col = self._pick_column(
                        production_columns, ("item_cost", "cost_amt")
                    )
                    prod_degree_expr = (
                        f"COALESCE(CAST({prod_degree_col} AS TEXT), '')"
                        if prod_degree_col
                        else "''"
                    )
                    sale_price_expr = (
                        f"COALESCE(CAST({sale_price_col} AS NUMERIC), 0)"
                        if sale_price_col
                        else "0"
                    )
                    item_cost_expr = (
                        f"COALESCE(CAST({item_cost_col} AS NUMERIC), 0)"
                        if item_cost_col
                        else "0"
                    )
                    rows = (
                        conn.execute(
                            text(
                                f"""
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(prod_qty::NUMERIC, 0)            AS "PROD_QTY",
                                CAST(prod_dt AS TEXT)                     AS "PROD_DT",
                                {prod_degree_expr}                        AS "PROD_DGRE",
                                {sale_price_expr}                         AS "SALE_PRC",
                                {item_cost_expr}                          AS "ITEM_COST"
                            FROM raw_production_extract
                            WHERE CAST(prod_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                            ),
                            {"date_from": date_from, "store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
                    production_data = [dict(r) for r in rows]

                if has_table(self.engine, "raw_daily_store_item_tmzon"):
                    rows = (
                        conn.execute(
                            text(
                                """
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(sale_qty::NUMERIC, 0)            AS "SALE_QTY",
                                CAST(sale_dt AS TEXT)                     AS "SALE_DT",
                                CAST(tmzon_div AS TEXT)                   AS "TMZON_DIV"
                            FROM raw_daily_store_item_tmzon
                            WHERE CAST(sale_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                            ),
                            {"date_from": date_from, "store_id": store_id},
                        )
                        .mappings()
                        .all()
                    )
                    sales_data = [dict(r) for r in rows]
        except (SQLAlchemyError, ValueError):
            return [], [], []

        return inventory_data, production_data, sales_data

    async def save_registration(self, payload: dict) -> dict:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.begin() as connection:
                    connection.execute(
                        text(
                            """
                            INSERT INTO production_registrations(
                                sku_id, qty, registered_by, feedback_type, feedback_message, store_id
                            ) VALUES (
                                :sku_id, :qty, :registered_by, :feedback_type, :feedback_message, :store_id
                            )
                            """
                        ),
                        {
                            **payload,
                            "feedback_type": "chance_loss_reduced",
                            "feedback_message": "재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
                        },
                    )
                    return payload
            except SQLAlchemyError:
                return {**payload, "saved": False}
        return {**payload, "saved": False}

    async def list_registration_history(
        self,
        limit: int = 20,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(
                        store_id=store_id, date_from=date_from, date_to=date_to
                    )
                    rows = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                sku_id,
                                qty,
                                registered_by,
                                COALESCE(feedback_type, 'chance_loss_reduced') AS feedback_type,
                                COALESCE(feedback_message, '재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.') AS feedback_message,
                                TO_CHAR(registered_at, 'YYYY-MM-DD HH24:MI:SS') AS registered_at,
                                store_id
                            FROM production_registrations
                            {filter_clause}
                            ORDER BY registered_at DESC
                            LIMIT :limit
                            """
                            ),
                            {"limit": limit, **params},
                        )
                        .mappings()
                        .all()
                    )
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                return []
        return []

    def get_stock_rate_recent_rows(self, store_id: str) -> list[dict]:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH ranked AS (
                                SELECT
                                    masked_stor_cd,
                                    prc_dt,
                                    item_cd,
                                    item_nm,
                                    COALESCE(ord_avg, 0) AS ord_avg,
                                    COALESCE(sal_avg, 0) AS sal_avg,
                                    COALESCE(stk_avg, 0) AS stk_avg,
                                    COALESCE(stk_rt, 0) AS stk_rt,
                                    DENSE_RANK() OVER (ORDER BY prc_dt DESC) AS dr
                                FROM core_stock_rate
                                WHERE masked_stor_cd = :store_id
                            )
                            SELECT
                                masked_stor_cd,
                                prc_dt,
                                item_cd,
                                item_nm,
                                ord_avg,
                                sal_avg,
                                stk_avg,
                                stk_rt,
                                dr
                            FROM ranked
                            WHERE dr <= 2
                            ORDER BY item_nm, dr
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows]
        except SQLAlchemyError as exc:
            logger.warning("get_stock_rate_recent_rows 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return []

    def get_stockout_latest_rows(self, store_id: str) -> list[dict]:
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH latest_date AS (
                                SELECT MAX(prc_dt) AS prc_dt
                                FROM core_stockout_time
                                WHERE masked_stor_cd = :store_id
                            )
                            SELECT
                                item_cd,
                                item_nm,
                                is_stockout,
                                stockout_hour
                            FROM core_stockout_time s
                            JOIN latest_date d ON s.prc_dt = d.prc_dt
                            WHERE s.masked_stor_cd = :store_id
                            """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows]
        except SQLAlchemyError as exc:
            logger.warning("get_stockout_latest_rows 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return []

    def get_inventory_status(
        self, store_id: str | None = None, page: int = 1, page_size: int = 10
    ) -> tuple[list[dict], int]:
        if not self.engine or not store_id:
            return [], 0
        try:
            offset = max(0, (page - 1) * page_size)
            with self.engine.connect() as conn:
                total_items = int(
                    conn.execute(
                        text(
                            """
                        SELECT COUNT(*) AS total_items
                        FROM (
                          SELECT item_nm
                          FROM raw_inventory_extract
                          WHERE masked_stor_cd = :store_id
                            AND stock_qty IS NOT NULL AND stock_qty != '' AND stock_qty != '0'
                          GROUP BY item_nm
                        ) AS grouped_items
                    """
                        ),
                        {"store_id": store_id},
                    ).scalar_one()
                )
                rows = (
                    conn.execute(
                        text(
                            """
                            WITH latest_date AS (
                                SELECT MAX(stock_dt) AS stock_dt
                                FROM raw_inventory_extract
                                WHERE masked_stor_cd = :store_id
                            )
                            SELECT
                                COALESCE(item_cd, item_nm) AS item_cd,
                                item_nm,
                                SUM(COALESCE(NULLIF(TRIM(disuse_qty), '')::numeric, 0)) AS total_disuse_qty,
                                AVG(COALESCE(NULLIF(TRIM(cost), '')::numeric, 0)) AS avg_cost
                            FROM raw_inventory_extract r
                            JOIN latest_date d ON r.stock_dt = d.stock_dt
                            WHERE r.masked_stor_cd = :store_id
                            GROUP BY COALESCE(item_cd, item_nm), item_nm
                            """
                        ),
                        {"store_id": store_id, "page_size": page_size, "offset": offset},
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows], total_items
        except SQLAlchemyError as exc:
            logger.warning("get_inventory_status 쿼리 실패: store_id=%s error=%s", store_id, exc)
            return [], 0

    async def get_registration_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(
                        store_id=store_id, date_from=date_from, date_to=date_to
                    )
                    recent_filter_clause, recent_params = self._build_history_filters(
                        store_id=store_id,
                        date_from=date_from or datetime.now().date().isoformat(),
                        date_to=date_to,
                    )
                    summary = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                COUNT(*) AS total,
                                COALESCE(SUM(qty), 0) AS total_registered_qty
                            FROM production_registrations
                            {filter_clause}
                            """
                            ),
                            params,
                        )
                        .mappings()
                        .one()
                    )
                    latest = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                sku_id,
                                qty,
                                registered_by,
                                COALESCE(feedback_type, 'chance_loss_reduced') AS feedback_type,
                                COALESCE(feedback_message, '재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.') AS feedback_message,
                                TO_CHAR(registered_at, 'YYYY-MM-DD HH24:MI:SS') AS registered_at,
                                store_id
                            FROM production_registrations
                            {filter_clause}
                            ORDER BY registered_at DESC
                            LIMIT 1
                            """
                            ),
                            params,
                        )
                        .mappings()
                        .first()
                    )
                    recent_registered_by = (
                        connection.execute(
                            text(
                                f"""
                            SELECT registered_by
                            FROM production_registrations
                            {filter_clause}
                            GROUP BY registered_by
                            ORDER BY MAX(registered_at) DESC
                            LIMIT 5
                            """
                            ),
                            params,
                        )
                        .scalars()
                        .all()
                    )
                    recent_7d_summary = (
                        connection.execute(
                            text(
                                f"""
                            SELECT
                                COUNT(*) AS recent_registration_count_7d,
                                COALESCE(SUM(qty), 0) AS recent_registered_qty_7d,
                                COUNT(DISTINCT sku_id) AS affected_sku_count
                            FROM production_registrations
                            {recent_filter_clause}
                            """
                            ),
                            recent_params,
                        )
                        .mappings()
                        .one()
                    )
                    return {
                        "total": int(summary["total"]),
                        "latest": dict(latest) if latest else None,
                        "total_registered_qty": int(summary["total_registered_qty"]),
                        "recent_registered_by": list(recent_registered_by),
                        "recent_registration_count_7d": int(
                            recent_7d_summary["recent_registration_count_7d"]
                        ),
                        "recent_registered_qty_7d": int(
                            recent_7d_summary["recent_registered_qty_7d"]
                        ),
                        "affected_sku_count": int(recent_7d_summary["affected_sku_count"]),
                        "summary_status": "active" if int(summary["total"]) > 0 else "empty",
                        "filtered_store_id": store_id,
                        "filtered_date_from": date_from,
                        "filtered_date_to": date_to,
                    }
            except SQLAlchemyError:
                return {
                    "total": 0,
                    "latest": None,
                    "total_registered_qty": 0,
                    "recent_registered_by": [],
                    "recent_registration_count_7d": 0,
                    "recent_registered_qty_7d": 0,
                    "affected_sku_count": 0,
                    "summary_status": "empty",
                    "filtered_store_id": store_id,
                    "filtered_date_from": date_from,
                    "filtered_date_to": date_to,
                }
        return {
            "total": 0,
            "latest": None,
            "total_registered_qty": 0,
            "recent_registered_by": [],
            "recent_registration_count_7d": 0,
            "recent_registered_qty_7d": 0,
            "affected_sku_count": 0,
            "summary_status": "empty",
            "filtered_store_id": store_id,
            "filtered_date_from": date_from,
            "filtered_date_to": date_to,
        }
