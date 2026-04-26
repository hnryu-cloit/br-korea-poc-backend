from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.infrastructure.db.utils import has_table


class CampaignRepositoryMixin:
    engine: Engine | None

    def _build_campaign_insight(self, campaign_context: dict) -> dict:
        group_detail = " / ".join(campaign_context["group_names"]) if campaign_context["group_names"] else "대상 그룹 정보 없음"
        item_detail = " / ".join(campaign_context["item_names"]) if campaign_context["item_names"] else "대상 상품 정보 없음"
        return {
            "title": "캠페인 시즌성 보정",
            "summary": (
                f"캠페인 마스터에서 사용 중인 캠페인 {campaign_context['campaign_count']}건을 확인했습니다. "
                f"대표 캠페인 {campaign_context['campaign_name']}은 {campaign_context['campaign_period']} 기간의 "
                f"{campaign_context['benefit_type']} 프로모션입니다."
            ),
            "metrics": [
                {"label": "사용 캠페인", "value": f"{campaign_context['campaign_count']}건", "detail": "캠페인 마스터 기준"},
                {"label": "대표 캠페인", "value": campaign_context["campaign_name"], "detail": campaign_context["campaign_period"]},
                {"label": "상품 그룹", "value": f"{campaign_context['group_count']}개", "detail": group_detail},
                {"label": "대상 상품", "value": f"{campaign_context['item_count']}개", "detail": item_detail},
            ],
            "actions": [
                "캠페인 대상 상품군과 메뉴 믹스 인사이트를 함께 비교해 주세요.",
                "행사 기간에는 채널 믹스와 결제수단 반응도 같이 점검해 주세요.",
            ],
            "status": "active",
        }

    @staticmethod
    def _is_tday_prompt(prompt_hint: str | None) -> bool:
        if not prompt_hint:
            return False
        lowered = prompt_hint.lower()
        return "티데이" in prompt_hint or "tday" in lowered

    @staticmethod
    def _to_date(value: str | None) -> date | None:
        if not value:
            return None
        normalized = value.replace("-", "")
        if len(normalized) != 8 or not normalized.isdigit():
            return None
        return datetime.strptime(normalized, "%Y%m%d").date()

    @staticmethod
    def _fmt_date(value: date | None) -> str | None:
        return value.strftime("%Y-%m-%d") if value else None

    @staticmethod
    def _safe_numeric(value: object) -> float:
        if value in (None, ""):
            return 0.0
        try:
            return max(float(value), 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _sum_sales_for_period(
        self,
        store_id: str | None,
        item_codes: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> float:
        if not self.engine or not start_date or not end_date:
            return 0.0

        clauses = ["sale_dt BETWEEN :start_dt AND :end_dt"]
        params: dict[str, object] = {
            "start_dt": start_date.strftime("%Y%m%d"),
            "end_dt": end_date.strftime("%Y%m%d"),
        }
        if store_id:
            clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id
        if item_codes:
            placeholders = []
            for index, code in enumerate(item_codes):
                key = f"item_code_{index}"
                params[key] = code
                placeholders.append(f":{key}")
            clauses.append(f"item_cd IN ({', '.join(placeholders)})")

        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        f"""
                        SELECT COALESCE(
                            SUM(CAST(COALESCE(NULLIF(CAST(actual_sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                            0
                        ) AS total_revenue
                        FROM raw_daily_store_item
                        WHERE {' AND '.join(clauses)}
                        """
                    ),
                    params,
                )
                .mappings()
                .first()
            )
        return self._safe_numeric((row or {}).get("total_revenue"))

    def _sum_campaign_discount_cost(self, campaign_code: str, store_id: str | None) -> float:
        if not self.engine or not has_table(self.engine, "raw_daily_store_cpi_tmzon"):
            return 0.0

        discount_expr = " + ".join(
            [f"COALESCE(NULLIF(dc_amt_{hour:02d}, '')::numeric, 0)" for hour in range(24)]
        )
        params: dict[str, object] = {"campaign_code": campaign_code}
        clauses = ["cpi_cd = :campaign_code"]
        if store_id:
            clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id

        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        f"""
                        SELECT COALESCE(SUM({discount_expr}), 0) AS total_discount_cost
                        FROM raw_daily_store_cpi_tmzon
                        WHERE {' AND '.join(clauses)}
                        """
                    ),
                    params,
                )
                .mappings()
                .first()
            )
        return self._safe_numeric((row or {}).get("total_discount_cost"))

    def _fetch_latest_tday_policy(self, reference_date: date | None) -> dict | None:
        if not self.engine or not has_table(self.engine, "raw_telecom_discount_policy"):
            return None

        ref = (reference_date or date.today()).strftime("%Y%m%d")
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """
                        SELECT
                            pay_dc_cd,
                            pay_dc_nm,
                            func_id_nm,
                            dc_apply_trgt_nm,
                            pay_dc_methd_nm,
                            start_dt,
                            fnsh_dt,
                            sales_org_cd
                        FROM raw_telecom_discount_policy
                        WHERE LOWER(COALESCE(pay_dc_nm, '')) LIKE '%tday%'
                           OR LOWER(COALESCE(func_id_nm, '')) LIKE '%tday%'
                        ORDER BY
                            CASE
                                WHEN :ref_dt BETWEEN COALESCE(start_dt, '00000000') AND COALESCE(fnsh_dt, '99999999') THEN 0
                                WHEN COALESCE(fnsh_dt, '00000000') < :ref_dt THEN 1
                                ELSE 2
                            END,
                            COALESCE(fnsh_dt, '00000000') DESC,
                            COALESCE(start_dt, '00000000') DESC
                        LIMIT 1
                        """
                    ),
                    {"ref_dt": ref},
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def _sum_payment_detail_amount(
        self,
        store_id: str | None,
        start_date: date | None,
        end_date: date | None,
        *,
        detail_names: list[str],
    ) -> float:
        if not self.engine or not start_date or not end_date or not detail_names:
            return 0.0
        if not has_table(self.engine, "raw_daily_store_pay_way"):
            return 0.0

        params: dict[str, object] = {
            "start_dt": start_date.strftime("%Y%m%d"),
            "end_dt": end_date.strftime("%Y%m%d"),
        }
        clauses = ["sale_dt BETWEEN :start_dt AND :end_dt"]
        if store_id:
            clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id
        placeholders = []
        for index, detail_name in enumerate(detail_names):
            key = f"detail_name_{index}"
            params[key] = detail_name
            placeholders.append(f":{key}")
        clauses.append(f"pay_dtl_cd_nm IN ({', '.join(placeholders)})")

        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        f"""
                        SELECT COALESCE(
                            SUM(CAST(COALESCE(NULLIF(CAST(pay_amt AS TEXT), ''), '0') AS NUMERIC)),
                            0
                        ) AS total_amount
                        FROM raw_daily_store_pay_way
                        WHERE {' AND '.join(clauses)}
                        """
                    ),
                    params,
                )
                .mappings()
                .first()
            )
        return self._safe_numeric((row or {}).get("total_amount"))

    def _fetch_product_mix_for_period(
        self,
        store_id: str | None,
        start_date: date | None,
        end_date: date | None,
        limit: int = 5,
    ) -> list[dict]:
        if not self.engine or not start_date or not end_date:
            return []

        params: dict[str, object] = {
            "start_dt": start_date.strftime("%Y%m%d"),
            "end_dt": end_date.strftime("%Y%m%d"),
            "limit": limit,
        }
        clauses = ["sale_dt BETWEEN :start_dt AND :end_dt"]
        if store_id:
            clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id

        with self.engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        f"""
                        WITH item_sales AS (
                            SELECT
                                item_cd,
                                COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), item_cd, '기타') AS item_nm,
                                COALESCE(
                                    SUM(CAST(COALESCE(NULLIF(CAST(actual_sale_amt AS TEXT), ''), '0') AS NUMERIC)),
                                    0
                                ) AS sales
                            FROM raw_daily_store_item
                            WHERE {' AND '.join(clauses)}
                            GROUP BY item_cd, item_nm
                        ),
                        totals AS (
                            SELECT COALESCE(SUM(sales), 0) AS total_sales
                            FROM item_sales
                        )
                        SELECT
                            i.item_cd,
                            i.item_nm,
                            i.sales,
                            CASE WHEN t.total_sales > 0 THEN ROUND((i.sales / t.total_sales) * 100, 1) ELSE 0 END AS share_pct
                        FROM item_sales i
                        CROSS JOIN totals t
                        ORDER BY i.sales DESC, i.item_nm
                        LIMIT :limit
                        """
                    ),
                    params,
                )
                .mappings()
                .all()
            )
        return [
            {
                "item_cd": row.get("item_cd"),
                "item_nm": row.get("item_nm"),
                "sales": self._safe_numeric(row.get("sales")),
                "share_pct": self._safe_numeric(row.get("share_pct")),
            }
            for row in rows
        ]

    def _fetch_previous_tday_periods(self, current_start: date, *, pay_dc_name: str | None) -> list[dict]:
        if not self.engine or not has_table(self.engine, "raw_telecom_discount_policy"):
            return []

        params = {"current_start": current_start.strftime("%Y%m%d")}
        name_clause = ""
        if pay_dc_name:
            params["pay_dc_nm"] = pay_dc_name
            name_clause = "AND pay_dc_nm = :pay_dc_nm"

        with self.engine.connect() as connection:
            exact_rows = (
                connection.execute(
                    text(
                        f"""
                        SELECT DISTINCT pay_dc_cd, pay_dc_nm, func_id_nm, dc_apply_trgt_nm, pay_dc_methd_nm, start_dt, fnsh_dt
                        FROM raw_telecom_discount_policy
                        WHERE COALESCE(fnsh_dt, '00000000') < :current_start
                          {name_clause}
                        ORDER BY fnsh_dt DESC, start_dt DESC
                        LIMIT 6
                        """
                    ),
                    params,
                )
                .mappings()
                .all()
            )
            if exact_rows:
                return [dict(row) for row in exact_rows]

            family_rows = (
                connection.execute(
                    text(
                        """
                        SELECT DISTINCT pay_dc_cd, pay_dc_nm, func_id_nm, dc_apply_trgt_nm, pay_dc_methd_nm, start_dt, fnsh_dt
                        FROM raw_telecom_discount_policy
                        WHERE COALESCE(fnsh_dt, '00000000') < :current_start
                          AND (
                                LOWER(COALESCE(pay_dc_nm, '')) LIKE '%tday%'
                             OR LOWER(COALESCE(func_id_nm, '')) LIKE '%tday%'
                          )
                        ORDER BY fnsh_dt DESC, start_dt DESC
                        LIMIT 6
                        """
                    ),
                    {"current_start": current_start.strftime("%Y%m%d")},
                )
                .mappings()
                .all()
            )
        return [dict(row) for row in family_rows]

    def _resolve_peer_store_ids(self, store_id: str) -> tuple[list[str], str]:
        if not self.engine or not has_table(self.engine, "store_clusters"):
            return [], "none"

        with self.engine.connect() as connection:
            cluster_row = (
                connection.execute(
                    text(
                        """
                        SELECT cluster_id
                        FROM store_clusters
                        WHERE masked_stor_cd = :store_id
                        LIMIT 1
                        """
                    ),
                    {"store_id": store_id},
                )
                .mappings()
                .first()
            )
            if not cluster_row or cluster_row.get("cluster_id") is None:
                return [], "none"

            rows = (
                connection.execute(
                    text(
                        """
                        SELECT masked_stor_cd
                        FROM store_clusters
                        WHERE cluster_id = :cluster_id
                          AND masked_stor_cd <> :store_id
                        ORDER BY masked_stor_cd
                        LIMIT 30
                        """
                    ),
                    {"cluster_id": cluster_row["cluster_id"], "store_id": store_id},
                )
                .mappings()
                .all()
            )
        return [str(row["masked_stor_cd"]) for row in rows], "cluster"

    def _build_tday_comparison(
        self,
        *,
        store_id: str,
        current_policy: dict,
        current_total_sales: float,
        current_usage_ratio_pct: float,
    ) -> dict:
        current_start = self._to_date(str(current_policy.get("start_dt") or ""))
        if not current_start:
            return {"basis": "none", "message": "비교 가능한 이전 유사 프로모션 데이터가 없습니다."}

        previous_periods = self._fetch_previous_tday_periods(
            current_start,
            pay_dc_name=str(current_policy.get("pay_dc_nm") or ""),
        )
        detail_names = ["SKT Tday 사용"]

        for policy in previous_periods:
            start_date = self._to_date(str(policy.get("start_dt") or ""))
            end_date = self._to_date(str(policy.get("fnsh_dt") or ""))
            if not start_date or not end_date:
                continue
            prior_sales = self._sum_sales_for_period(store_id, [], start_date, end_date)
            if prior_sales <= 0:
                continue
            prior_usage = self._sum_payment_detail_amount(store_id, start_date, end_date, detail_names=detail_names)
            prior_ratio = round((prior_usage / prior_sales) * 100, 1) if prior_sales > 0 else 0.0
            return {
                "basis": "same_store_prior_similar",
                "message": f"직전 유사 프로모션({self._fmt_date(start_date)} ~ {self._fmt_date(end_date)})과 비교했습니다.",
                "benchmark_sales": prior_sales,
                "benchmark_usage_ratio_pct": self._safe_numeric(prior_ratio),
                "sales_change_pct": round(((current_total_sales - prior_sales) / prior_sales) * 100, 1),
                "usage_ratio_gap_pct": round(current_usage_ratio_pct - prior_ratio, 1),
                "benchmark_period": f"{self._fmt_date(start_date)} ~ {self._fmt_date(end_date)}",
            }

        peer_store_ids, _ = self._resolve_peer_store_ids(store_id)
        if not peer_store_ids:
            return {"basis": "none", "message": "해당 매장과 비교할 유사 점포군 데이터가 없습니다."}

        aggregated_sales: list[float] = []
        aggregated_ratios: list[float] = []
        for policy in previous_periods:
            start_date = self._to_date(str(policy.get("start_dt") or ""))
            end_date = self._to_date(str(policy.get("fnsh_dt") or ""))
            if not start_date or not end_date:
                continue
            for peer_store_id in peer_store_ids:
                peer_sales = self._sum_sales_for_period(peer_store_id, [], start_date, end_date)
                if peer_sales <= 0:
                    continue
                peer_usage = self._sum_payment_detail_amount(peer_store_id, start_date, end_date, detail_names=detail_names)
                aggregated_sales.append(peer_sales)
                aggregated_ratios.append(round((peer_usage / peer_sales) * 100, 1) if peer_sales > 0 else 0.0)

        if not aggregated_sales:
            return {"basis": "none", "message": "유사 점포군의 이전 유사 프로모션 비교 데이터가 없습니다."}

        benchmark_sales = self._safe_numeric(sum(aggregated_sales) / len(aggregated_sales))
        benchmark_ratio = self._safe_numeric(sum(aggregated_ratios) / len(aggregated_ratios))
        return {
            "basis": "peer_average_prior_similar",
            "message": (
                "해당 매장에서 이전 유사 프로모션 정보가 없어 타 매장의 평균치와 비교합니다. "
                "비교 기준은 동일 시도 + 점포유형 기준 유사 점포군입니다."
            ),
            "benchmark_sales": benchmark_sales,
            "benchmark_usage_ratio_pct": round(benchmark_ratio, 1),
            "sales_change_pct": round(((current_total_sales - benchmark_sales) / benchmark_sales) * 100, 1) if benchmark_sales > 0 else 0.0,
            "usage_ratio_gap_pct": round(current_usage_ratio_pct - benchmark_ratio, 1),
            "peer_group": "동일 시도 + 점포유형",
            "peer_store_count": len(peer_store_ids),
        }

    def _build_tday_effect(self, *, store_id: str, reference_date: date | None) -> dict:
        policy = self._fetch_latest_tday_policy(reference_date)
        if not policy:
            raise LookupError("티데이 프로모션 데이터를 찾을 수 없습니다.")

        during_start = self._to_date(str(policy.get("start_dt") or ""))
        during_end = self._to_date(str(policy.get("fnsh_dt") or ""))
        if not during_start or not during_end:
            raise LookupError("티데이 프로모션 기간 정보를 찾을 수 없습니다.")

        total_sales = self._sum_sales_for_period(store_id, [], during_start, during_end)
        usage_amount = self._sum_payment_detail_amount(
            store_id,
            during_start,
            during_end,
            detail_names=["SKT Tday 사용"],
        )
        total_sales = self._safe_numeric(total_sales)
        usage_amount = self._safe_numeric(usage_amount)
        usage_ratio_pct = round((usage_amount / total_sales) * 100, 1) if total_sales > 0 else 0.0
        product_mix = self._fetch_product_mix_for_period(store_id, during_start, during_end)
        for item in product_mix:
            item["sales"] = self._safe_numeric(item.get("sales"))
            item["share_pct"] = self._safe_numeric(item.get("share_pct"))
        comparison = self._build_tday_comparison(
            store_id=store_id,
            current_policy=policy,
            current_total_sales=total_sales,
            current_usage_ratio_pct=usage_ratio_pct,
        )
        return {
            "analysis_mode": "telecom_tday",
            "campaign_code": str(policy.get("pay_dc_cd") or ""),
            "campaign_name": str(policy.get("pay_dc_nm") or "SKT Tday"),
            "benefit_type": str(policy.get("pay_dc_methd_nm") or "율"),
            "discount_cost": usage_amount,
            "uplift_revenue": 0.0,
            "roi_pct": usage_ratio_pct,
            "payback_days": None,
            "promotion_period_sales": total_sales,
            "usage_amount": usage_amount,
            "usage_ratio_pct": usage_ratio_pct,
            "product_mix": product_mix,
            "comparison": comparison,
            "periods": [
                {
                    "label": "프로모션 기간",
                    "start_date": self._fmt_date(during_start),
                    "end_date": self._fmt_date(during_end),
                    "revenue": total_sales,
                }
            ],
        }

    async def get_campaign_effect(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        prompt_hint: str | None = None,
    ) -> dict:
        if store_id and self._is_tday_prompt(prompt_hint):
            reference_date = self._to_date(date_to or date_from)
            return self._build_tday_effect(store_id=store_id, reference_date=reference_date)

        # 1순위: mart_campaign_effect_daily 사용 (활성/비활성 모두 처리)
        if self.engine and has_table(self.engine, "mart_campaign_effect_daily"):
            mart_payload = self._build_campaign_effect_from_mart(
                date_from=date_from, date_to=date_to
            )
            if mart_payload:
                return mart_payload

        campaign_context = self._fetch_campaign_context()
        if not campaign_context:
            raise LookupError("캠페인 효과 데이터가 없습니다.")

        campaign_code = self._first_text(campaign_context.get("campaign_cd"))
        campaign_name = self._first_text(campaign_context.get("campaign_name"), campaign_code, "대표 캠페인")
        benefit_type = self._first_text(campaign_context.get("benefit_type"), "캠페인")

        item_codes: list[str] = []
        if self.engine and has_table(self.engine, "raw_campaign_item") and campaign_code:
            with self.engine.connect() as connection:
                item_rows = (
                    connection.execute(
                        text(
                            """
                            SELECT DISTINCT item_cd
                            FROM raw_campaign_item
                            WHERE cpi_cd = :campaign_code
                              AND item_cd IS NOT NULL
                              AND item_cd <> ''
                            """
                        ),
                        {"campaign_code": campaign_code},
                    )
                    .mappings()
                    .all()
                )
            item_codes = [str(row["item_cd"]) for row in item_rows if row.get("item_cd")]

        campaign_start: date | None = None
        campaign_end: date | None = None
        if self.engine and has_table(self.engine, "raw_campaign_master") and campaign_code:
            with self.engine.connect() as connection:
                period_row = (
                    connection.execute(
                        text(
                            """
                            SELECT start_dt, fnsh_dt
                            FROM raw_campaign_master
                            WHERE cpi_cd = :campaign_code
                            ORDER BY COALESCE(fnsh_dt, ''), COALESCE(start_dt, '') DESC
                            LIMIT 1
                            """
                        ),
                        {"campaign_code": campaign_code},
                    )
                    .mappings()
                    .first()
                )
            if period_row:
                campaign_start = self._to_date(self._first_text(period_row.get("start_dt")))
                campaign_end = self._to_date(self._first_text(period_row.get("fnsh_dt")))
        if not campaign_start or not campaign_end:
            period_tokens = self._first_text(campaign_context.get("campaign_period")).split(" ~ ")
            campaign_start = campaign_start or self._to_date(period_tokens[0] if period_tokens else None)
            campaign_end = campaign_end or self._to_date(period_tokens[-1] if period_tokens else None)

        selected_from = self._to_date(date_from)
        selected_to = self._to_date(date_to)
        during_start = selected_from or campaign_start
        during_end = selected_to or campaign_end
        if during_start and during_end and during_start > during_end:
            during_start, during_end = during_end, during_start
        if not during_start and during_end:
            during_start = during_end - timedelta(days=6)
        if during_start and not during_end:
            during_end = during_start + timedelta(days=6)

        period_days = ((during_end - during_start).days + 1) if during_start and during_end else 7
        pre_end = during_start - timedelta(days=1) if during_start else None
        pre_start = pre_end - timedelta(days=period_days - 1) if pre_end else None
        post_start = during_end + timedelta(days=1) if during_end else None
        post_end = post_start + timedelta(days=period_days - 1) if post_start else None

        pre_revenue = self._sum_sales_for_period(store_id, item_codes, pre_start, pre_end)
        during_revenue = self._sum_sales_for_period(store_id, item_codes, during_start, during_end)
        post_revenue = self._sum_sales_for_period(store_id, item_codes, post_start, post_end)
        discount_cost = self._sum_campaign_discount_cost(campaign_code, store_id)
        uplift_revenue = during_revenue - pre_revenue
        roi_pct = round(((uplift_revenue - discount_cost) / discount_cost) * 100, 1) if discount_cost > 0 else 0.0
        daily_uplift = uplift_revenue / max(period_days, 1)
        payback_days = round(discount_cost / daily_uplift, 1) if daily_uplift > 0 and discount_cost > 0 else None

        return {
            "campaign_code": campaign_code,
            "campaign_name": campaign_name,
            "benefit_type": benefit_type,
            "discount_cost": discount_cost,
            "uplift_revenue": uplift_revenue,
            "roi_pct": roi_pct,
            "payback_days": payback_days,
            "periods": [
                {"label": "캠페인 전", "start_date": self._fmt_date(pre_start), "end_date": self._fmt_date(pre_end), "revenue": pre_revenue},
                {"label": "캠페인 중", "start_date": self._fmt_date(during_start), "end_date": self._fmt_date(during_end), "revenue": during_revenue},
                {"label": "캠페인 후", "start_date": self._fmt_date(post_start), "end_date": self._fmt_date(post_end), "revenue": post_revenue},
            ],
        }

    def _build_campaign_effect_from_mart(
        self,
        date_from: str | None,
        date_to: str | None,
    ) -> dict | None:
        """mart_campaign_effect_daily 기반 활성/비활성 캠페인 효과 페이로드 산출.

        - 기준 기간(date_from~date_to) 내 mart 행이 있으면 '진행 중' 모드.
        - 기간 내 행이 없으면 폴백 조회 없이 단일 안내 페이로드(no_active=True) 반환.
        """
        if not self.engine:
            return None

        normalized_from = self._normalize_yyyymmdd(date_from)
        normalized_to = self._normalize_yyyymmdd(date_to)
        if not normalized_to:
            normalized_to = self._fetch_latest_mart_dt()
        if not normalized_from and normalized_to:
            normalized_from = self._shift_yyyymmdd(normalized_to, -27)
        if not normalized_from or not normalized_to:
            return None

        active_rows = self._fetch_mart_window(normalized_from, normalized_to)
        window_label_from = normalized_from
        window_label_to = normalized_to
        if not active_rows:
            return {
                "campaign_code": "-",
                "campaign_name": "진행 중 캠페인 없음",
                "benefit_type": "기준 기간 내 활성 캠페인 부재",
                "discount_cost": 0,
                "uplift_revenue": 0,
                "roi_pct": 0.0,
                "payback_days": None,
                "active_campaign_count": 0,
                "no_active_campaigns": True,
                "avg_lift_ratio": 0.0,
                "total_sales_during": 0,
                "top_campaigns": [],
                "window_from": window_label_from,
                "window_to": window_label_to,
                "periods": [
                    {
                        "label": "기간 내 캠페인 매출",
                        "start_date": self._fmt_yyyymmdd(window_label_from),
                        "end_date": self._fmt_yyyymmdd(window_label_to),
                        "revenue": 0,
                    }
                ],
            }

        # 캠페인 단위 집계
        by_campaign: dict[str, dict] = {}
        for row in active_rows:
            cpi = str(row["cpi_cd"])
            bucket = by_campaign.setdefault(
                cpi,
                {
                    "cpi_cd": cpi,
                    "cpi_nm": row.get("cpi_nm") or cpi,
                    "sales": 0.0,
                    "dc_amt": 0.0,
                    "qty": 0.0,
                    "lift_sum": 0.0,
                    "lift_days": 0,
                    "active_days": 0,
                    "first_dt": row["sale_dt"],
                    "last_dt": row["sale_dt"],
                    "stores": int(row.get("participating_store_count") or 0),
                    "applicable_items": int(row.get("applicable_item_count") or 0),
                },
            )
            bucket["sales"] += float(row.get("total_sales_during") or 0)
            bucket["dc_amt"] += float(row.get("total_dc_amt") or 0)
            bucket["qty"] += float(row.get("total_qty") or 0)
            lift = row.get("sales_lift_ratio")
            if lift is not None:
                bucket["lift_sum"] += float(lift)
                bucket["lift_days"] += 1
            bucket["active_days"] += 1
            if str(row["sale_dt"]) < str(bucket["first_dt"]):
                bucket["first_dt"] = row["sale_dt"]
            if str(row["sale_dt"]) > str(bucket["last_dt"]):
                bucket["last_dt"] = row["sale_dt"]
            stores_val = int(row.get("participating_store_count") or 0)
            if stores_val > bucket["stores"]:
                bucket["stores"] = stores_val
            apps_val = int(row.get("applicable_item_count") or 0)
            if apps_val > bucket["applicable_items"]:
                bucket["applicable_items"] = apps_val

        ranked = sorted(by_campaign.values(), key=lambda x: x["sales"], reverse=True)
        active_count = len(ranked)
        total_sales = sum(c["sales"] for c in ranked)
        total_dc = sum(c["dc_amt"] for c in ranked)
        avg_lift = (
            sum(c["lift_sum"] for c in ranked)
            / max(sum(c["lift_days"] for c in ranked), 1)
        )

        top = ranked[0] if ranked else None
        roi_pct = (
            round(((total_sales - total_dc) / total_dc) * 100, 1)
            if total_dc > 0
            else 0.0
        )

        top_campaigns = [
            {
                "cpi_cd": c["cpi_cd"],
                "cpi_nm": c["cpi_nm"],
                "sales": round(c["sales"], 0),
                "dc_amt": round(c["dc_amt"], 0),
                "active_days": c["active_days"],
                "avg_lift_ratio": (
                    round(c["lift_sum"] / c["lift_days"], 4) if c["lift_days"] else 0.0
                ),
                "applicable_items": c["applicable_items"],
            }
            for c in ranked[:5]
        ]

        if active_count == 1 and top:
            campaign_name = top["cpi_nm"]
        else:
            campaign_name = (
                f"{active_count}건 진행 중 (대표: {top['cpi_nm']})"
                if top
                else f"{active_count}건"
            )

        return {
            "campaign_code": top["cpi_cd"] if top else "-",
            "campaign_name": campaign_name,
            "benefit_type": "기간 내 캠페인 통합",
            "discount_cost": round(total_dc, 0),
            "uplift_revenue": round(total_sales - total_dc, 0),
            "roi_pct": roi_pct,
            "payback_days": None,
            "active_campaign_count": active_count,
            "no_active_campaigns": False,
            "avg_lift_ratio": round(avg_lift, 4),
            "total_sales_during": round(total_sales, 0),
            "top_campaigns": top_campaigns,
            "window_from": window_label_from,
            "window_to": window_label_to,
            "periods": [
                {
                    "label": "기간 내 캠페인 매출",
                    "start_date": self._fmt_yyyymmdd(window_label_from),
                    "end_date": self._fmt_yyyymmdd(window_label_to),
                    "revenue": round(total_sales, 0),
                }
            ],
        }

    def _fetch_mart_window(self, dt_from: str, dt_to: str) -> list[dict]:
        sql = text(
            """
            SELECT cpi_cd, cpi_nm, sale_dt, total_sales_during, total_dc_amt,
                   total_qty, sales_lift_ratio, applicable_item_count,
                   participating_store_count
            FROM mart_campaign_effect_daily
            WHERE sale_dt BETWEEN :dt_from AND :dt_to
              AND total_sales_during > 0
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"dt_from": dt_from, "dt_to": dt_to}).mappings().all()
        return [dict(r) for r in rows]

    def _fetch_latest_mart_dt(self) -> str | None:
        sql = text("SELECT MAX(sale_dt) AS max_dt FROM mart_campaign_effect_daily")
        with self.engine.connect() as conn:
            row = conn.execute(sql).mappings().first()
        return row["max_dt"] if row and row.get("max_dt") else None

    @staticmethod
    def _normalize_yyyymmdd(value: str | None) -> str | None:
        if not value:
            return None
        cleaned = str(value).replace("-", "").strip()
        return cleaned if len(cleaned) == 8 and cleaned.isdigit() else None

    @staticmethod
    def _shift_yyyymmdd(value: str, days: int) -> str:
        base = datetime.strptime(value, "%Y%m%d")
        return (base + timedelta(days=days)).strftime("%Y%m%d")

    @staticmethod
    def _fmt_yyyymmdd(value: str | None) -> str:
        if not value or len(str(value)) != 8:
            return ""
        s = str(value)
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
