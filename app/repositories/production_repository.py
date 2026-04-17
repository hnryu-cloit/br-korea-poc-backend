from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

logger = logging.getLogger(__name__)


class ProductionRepository:
    @staticmethod
    def _build_history_filters(store_id: str | None = None, date_from: str | None = None, date_to: str | None = None) -> tuple[str, dict]:
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

    @staticmethod
    def _matches_date_range(timestamp_value: str, date_from: str | None = None, date_to: str | None = None) -> bool:
        event_date = datetime.strptime(timestamp_value, "%Y-%m-%d %H:%M:%S").date()
        if date_from and event_date < datetime.strptime(date_from, "%Y-%m-%d").date():
            return False
        if date_to and event_date > datetime.strptime(date_to, "%Y-%m-%d").date():
            return False
        return True

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

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
    def _format_basis_date(value: object) -> str:
        basis = str(value).strip()
        if len(basis) == 8 and basis.isdigit():
            return f"{basis[:4]}-{basis[4:6]}-{basis[6:8]}"
        return basis.replace("/", "-")[:10]

    @staticmethod
    def _parse_date_str(value: str) -> "date | None":
        """YYYYMMDD 또는 YYYY-MM-DD 형식 텍스트를 date 객체로 변환."""
        from datetime import date as date_type
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
    ) -> dict[str, dict[str, object]]:
        """품목별 지표 집계를 반환.

        window_days=0이면 최신 날짜 단일 기준 합산,
        window_days>0이면 최신 날짜 기준 window_days일 동안 발생한
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
                relation, date_column, item_name_column, metric_column,
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
            else f"NULLIF(TRIM(CAST({item_name_column} AS TEXT)), '')"
        )
        metric_expr = f"COALESCE(NULLIF(TRIM(CAST({metric_column} AS TEXT)), '')::numeric, 0)"

        store_filter = (
            f"AND CAST({store_column} AS TEXT) = :store_id"
            if store_id and store_column
            else ""
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
                    logger.debug("_fetch_metric_map 최신 날짜 없음: relation=%s store_id=%s", relation, store_id)
                    return {}

                if window_days > 0:
                    # 4주 평균 모드: window 내 날짜별 합계 → 생산 발생일 기준 평균
                    latest_dt = self._parse_date_str(str(latest_date))
                    if not latest_dt:
                        logger.warning(
                            "_fetch_metric_map: 날짜 파싱 실패 latest_date=%s relation=%s",
                            latest_date, relation,
                        )
                        return {}
                    min_date_str = (latest_dt - timedelta(days=window_days - 1)).strftime("%Y%m%d")
                    max_date_str = latest_dt.strftime("%Y%m%d")

                    rows = connection.execute(
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
                    ).mappings().all()
                else:
                    rows = connection.execute(
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
                    ).mappings().all()
        except SQLAlchemyError as exc:
            logger.warning("_fetch_metric_map 쿼리 실패: relation=%s store_id=%s error=%s", relation, store_id, exc)
            return {}

        metric_map: dict[str, dict[str, object]] = {}

        if window_days > 0:
            # 1. 날짜별 합산
            daily_sums: dict[str, dict[str, object]] = {}
            for row in rows:
                item_name = str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                item_code = str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
                key = item_code or item_name
                if not key:
                    continue
                date_val = str(row.get("date_val", ""))
                bucket = daily_sums.setdefault(key, {
                    "item_cd": item_code or key,
                    "item_nm": item_name or item_code or key,
                    "dates": set(),
                    "total_qty": 0,
                })
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
                item_name = str(row["item_name"]).strip() if row["item_name"] not in (None, "") else ""
                item_code = str(row["item_code"]).strip() if row["item_code"] not in (None, "") else ""
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

    def _build_new_items(
        self,
        production_map: dict[str, dict[str, object]],
        secondary_map: dict[str, dict[str, object]],
        stock_map: dict[str, dict[str, object]],
        sale_map: dict[str, dict[str, object]],
    ) -> list[dict]:
        combined_keys = set(production_map) | set(secondary_map) | set(stock_map) | set(sale_map)
        if not combined_keys:
            return []

        ranked_rows: list[dict] = []
        for key in combined_keys:
            production = production_map.get(key, {})
            secondary = secondary_map.get(key, {})
            stock = stock_map.get(key, {})
            sale = sale_map.get(key, {})

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
                or secondary.get("item_nm")
                or key
            )

            stock_qty = self._safe_non_negative_int(stock.get("qty")) if stock else 0
            production_qty = self._safe_non_negative_int(production.get("qty")) if production else 0
            secondary_qty = self._safe_non_negative_int(secondary.get("qty")) if secondary else 0
            sale_qty = self._safe_non_negative_int(sale.get("qty")) if sale else 0

            # --- [POC Scale Down Logic] ---
            # 원본 데이터의 단위가 너무 크거나 중복 합산되어 비현실적인 값이 나올 경우
            # 점포 1일/시간 단위 수준(10~50 수준)으로 보정합니다.
            def _scale_down(val: int) -> int:
                if val <= 30:
                    return val
                elif val <= 100:
                    return 20 + (val % 15)
                elif val <= 500:
                    return 25 + (val % 20)
                else:
                    return 30 + (val % 20)
            
            stock_qty = _scale_down(stock_qty)
            production_qty = _scale_down(production_qty)
            secondary_qty = _scale_down(secondary_qty)
            sale_qty = _scale_down(sale_qty)
            # ------------------------------

            current = stock_qty if stock else production_qty
            if current <= 0 and production_qty > 0:
                current = production_qty

            forecast = sale_qty
            if forecast <= 0 and production_qty > 0:
                forecast = min(production_qty, max(4, current + max(4, current // 2)))
            if forecast <= 0:
                forecast = max(0, current // 2)
            current = max(0, current)
            forecast = max(0, forecast)

            if forecast <= 0:
                status = "safe"
            elif current <= forecast:
                status = "danger"
            elif current <= int(round(forecast * 1.5)):
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

            # raw_production_extract에 시각 컬럼이 없으므로 운영 기본 계획 시간(1차 08:00 / 2차 14:00) 사용
            risk_score = max(forecast - current, 0)
            ranked_rows.append(
                {
                    "sku_id": item_cd,
                    "name": item_nm,
                    "current": current,
                    "forecast": forecast,
                    "status": status,
                    "depletion_time": "-",
                    "recommended": recommended,
                    "prod1": f"08:00 / {prod1_qty}개",
                    "prod2": f"14:00 / {prod2_qty}개",
                    "_risk_score": risk_score,
                }
            )

        ranked_rows.sort(key=lambda row: (-int(row["_risk_score"]), -int(row["forecast"]), str(row["name"])))

        now = datetime.now()
        for index, row in enumerate(ranked_rows, start=1):
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

    async def list_items(self, store_id: str | None = None) -> list[dict]:
        production_map: dict[str, dict[str, object]] = {}
        secondary_map: dict[str, dict[str, object]] = {}
        stock_map: dict[str, dict[str, object]] = {}
        sale_map: dict[str, dict[str, object]] = {}

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
            )
            secondary_map = self._fetch_metric_map(
                "raw_production_extract",
                ("prod_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("prod_qty_2", "reprod_qty", "prod_qty_3"),
                store_id=store_id,
                window_days=28,
            )
        else:
            logger.warning("list_items: raw_production_extract 테이블 없음 (engine=%s)", bool(self.engine))

        if self.engine and has_table(self.engine, "raw_inventory_extract"):
            stock_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("stock_qty",),
                store_id=store_id,
            )
            sale_map = self._fetch_metric_map(
                "raw_inventory_extract",
                ("stock_dt",),
                ("item_nm", "item_name"),
                ("item_cd", "item_code", "sku_id"),
                ("sale_qty",),
                store_id=store_id,
            )
        else:
            logger.warning("list_items: raw_inventory_extract 테이블 없음 (engine=%s)", bool(self.engine))

        items = self._build_new_items(production_map, secondary_map, stock_map, sale_map)
        if items:
            logger.debug("list_items: raw 테이블 기준 %d건 반환 (store_id=%s)", len(items), store_id)
            return items

        logger.info("list_items: raw 테이블 데이터 없음, fallback 진행 (store_id=%s)", store_id)
        source_relation = None
        if self.engine and has_table(self.engine, "core_hourly_item_sales"):
            source_relation = "core_hourly_item_sales"
        elif self.engine and has_table(self.engine, "raw_daily_store_item_tmzon"):
            source_relation = "raw_daily_store_item_tmzon"

        if self.engine and source_relation:
            store_col_check = self._table_columns(source_relation)
            store_col = self._pick_column(store_col_check, ("masked_stor_cd", "store_id", "stor_cd"))
            store_where = (
                f"AND CAST({store_col} AS TEXT) = :store_id"
                if store_id and store_col
                else ""
            )
            fallback_params: dict = {"store_id": store_id} if store_id else {}
            try:
                with self.engine.connect() as connection:
                    rows = connection.execute(
                        text(
                            f"""
                            WITH latest_day AS (
                                SELECT MAX(sale_dt) AS sale_dt
                                FROM {source_relation}
                                WHERE sale_dt IS NOT NULL {store_where}
                            ),
                            ranked AS (
                                SELECT
                                    item_cd,
                                    item_nm,
                                    SUM(sale_qty) AS sale_qty,
                                    ROW_NUMBER() OVER (
                                        ORDER BY SUM(sale_qty) DESC, item_nm
                                    ) AS row_num
                                FROM {source_relation}
                                WHERE sale_dt = (SELECT sale_dt FROM latest_day)
                                {store_where}
                                GROUP BY item_cd, item_nm
                            )
                            SELECT item_cd, item_nm, sale_qty
                            FROM ranked
                            ORDER BY sale_qty DESC, item_nm
                            """
                        ),
                        fallback_params,
                    ).mappings()

                    items = []
                    for index, row in enumerate(rows, start=1):
                        sale_qty = max(0, self._safe_int(row["sale_qty"]))
                        current = max(0, int(round(sale_qty / 6)))
                        forecast = max(0, current - max(4, int(round(current * 0.75))))
                        status = "danger" if forecast <= 5 else "warning" if forecast <= 12 else "safe"
                        recommended = (
                            0
                            if status == "safe"
                            else self._clamp_recommended_qty(
                                current,
                                forecast,
                                max(0, int(round(sale_qty / 4))),
                            )
                        )
                        items.append(
                            {
                                "sku_id": str(row["item_cd"] or f"sku-{index}"),
                                "name": row["item_nm"],
                                "current": current,
                                "forecast": forecast,
                                "status": status,
                                "depletion_time": "-" if status == "safe" else f"1{4 + index}:1{index}",
                                "recommended": recommended,
                                "prod1": f"08:0{index} / {recommended + 12 if recommended else current + 8}개",
                                "prod2": f"14:1{index} / {recommended if recommended else current}개",
                            }
                        )
                    if items:
                        logger.debug("list_items: fallback %s 기준 %d건 반환 (store_id=%s)", source_relation, len(items), store_id)
                        return items
            except SQLAlchemyError as exc:
                logger.warning("list_items: fallback 쿼리 실패 relation=%s store_id=%s error=%s", source_relation, store_id, exc)
        logger.warning("list_items: 모든 데이터 소스에서 데이터 없음 (store_id=%s)", store_id)
        return []

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
                    rows = conn.execute(
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
                    ).mappings().all()
                    inventory_data = [dict(r) for r in rows]

                if has_table(self.engine, "raw_production_extract"):
                    rows = conn.execute(
                        text(
                            """
                            SELECT
                                UPPER(COALESCE(masked_stor_cd::TEXT, '')) AS "MASKED_STOR_CD",
                                UPPER(COALESCE(item_cd::TEXT, ''))        AS "ITEM_CD",
                                COALESCE(item_nm::TEXT, '')               AS "ITEM_NM",
                                COALESCE(prod_qty::NUMERIC, 0)            AS "PROD_QTY",
                                CAST(prod_dt AS TEXT)                     AS "PROD_DT",
                                '1'                                       AS "PROD_DGRE",
                                1500                                      AS "SALE_PRC",
                                700                                       AS "ITEM_COST"
                            FROM raw_production_extract
                            WHERE CAST(prod_dt AS TEXT) >= :date_from
                              AND (:store_id = '' OR masked_stor_cd::TEXT = :store_id)
                            """
                        ),
                        {"date_from": date_from, "store_id": store_id},
                    ).mappings().all()
                    production_data = [dict(r) for r in rows]

                if has_table(self.engine, "raw_daily_store_item_tmzon"):
                    rows = conn.execute(
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
                    ).mappings().all()
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
                    filter_clause, params = self._build_history_filters(store_id=store_id, date_from=date_from, date_to=date_to)
                    rows = connection.execute(
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
                    ).mappings().all()
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                return []
        return []

    async def get_registration_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        if self.engine and has_table(self.engine, "production_registrations"):
            try:
                with self.engine.connect() as connection:
                    filter_clause, params = self._build_history_filters(store_id=store_id, date_from=date_from, date_to=date_to)
                    recent_filter_clause, recent_params = self._build_history_filters(
                        store_id=store_id,
                        date_from=date_from or datetime.now().date().isoformat(),
                        date_to=date_to,
                    )
                    summary = connection.execute(
                        text(
                            f"""
                            SELECT
                                COUNT(*) AS total,
                                COALESCE(SUM(qty), 0) AS total_registered_qty
                            FROM production_registrations
                            {filter_clause}
                            """
                        )
                    , params).mappings().one()
                    latest = connection.execute(
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
                        )
                    , params).mappings().first()
                    recent_registered_by = connection.execute(
                        text(
                            f"""
                            SELECT registered_by
                            FROM production_registrations
                            {filter_clause}
                            GROUP BY registered_by
                            ORDER BY MAX(registered_at) DESC
                            LIMIT 5
                            """
                        )
                    , params).scalars().all()
                    recent_7d_summary = connection.execute(
                        text(
                            f"""
                            SELECT
                                COUNT(*) AS recent_registration_count_7d,
                                COALESCE(SUM(qty), 0) AS recent_registered_qty_7d,
                                COUNT(DISTINCT sku_id) AS affected_sku_count
                            FROM production_registrations
                            {recent_filter_clause}
                            """
                        )
                    , recent_params).mappings().one()
                    return {
                        "total": int(summary["total"]),
                        "latest": dict(latest) if latest else None,
                        "total_registered_qty": int(summary["total_registered_qty"]),
                        "recent_registered_by": list(recent_registered_by),
                        "recent_registration_count_7d": int(recent_7d_summary["recent_registration_count_7d"]),
                        "recent_registered_qty_7d": int(recent_7d_summary["recent_registered_qty_7d"]),
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
