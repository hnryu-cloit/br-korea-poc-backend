from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.infrastructure.db.utils import has_table


class CampaignRepositoryMixin:
    engine: Engine | None
    def _build_campaign_insight(self, campaign_context: dict) -> dict:
        group_detail = (
            " / ".join(campaign_context["group_names"])
            if campaign_context["group_names"]
            else "대상 그룹 정보 없음"
        )
        item_detail = (
            " / ".join(campaign_context["item_names"])
            if campaign_context["item_names"]
            else "대상 상품 정보 없음"
        )
        return {
            "title": "캠페인 시즌성 보정",
            "summary": (
                f"캠페인 마스터에서 사용 중인 캠페인 {campaign_context['campaign_count']}건을 확인했습니다. "
                f"대표 캠페인 {campaign_context['campaign_name']}은 {campaign_context['campaign_period']} 기간의 "
                f"{campaign_context['benefit_type']} 프로모션입니다."
            ),
            "metrics": [
                {
                    "label": "사용 캠페인",
                    "value": f"{campaign_context['campaign_count']}건",
                    "detail": "캠페인 마스터 기준",
                },
                {
                    "label": "대표 캠페인",
                    "value": campaign_context["campaign_name"],
                    "detail": campaign_context["campaign_period"],
                },
                {
                    "label": "상품 그룹",
                    "value": f"{campaign_context['group_count']}개",
                    "detail": group_detail,
                },
                {
                    "label": "대상 상품",
                    "value": f"{campaign_context['item_count']}개",
                    "detail": item_detail,
                },
            ],
            "actions": [
                "캠페인 대상 상품군은 메뉴 믹스 인사이트와 함께 비교해 주세요.",
                "행사 기간에는 채널 믹스와 결제수단 반응을 같이 점검해 주세요.",
            ],
            "status": "active",
        }

    @staticmethod
    def _as_yyyymmdd(value: str | None) -> str | None:
        if not value:
            return None
        return value.replace("-", "")

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
        if not value:
            return None
        return value.strftime("%Y-%m-%d")

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
            code_params = []
            for index, code in enumerate(item_codes):
                key = f"item_code_{index}"
                code_params.append(f":{key}")
                params[key] = code
            clauses.append(f"item_cd IN ({', '.join(code_params)})")

        where_clause = " AND ".join(clauses)
        try:
            with self.engine.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            f"""
                        SELECT COALESCE(SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)), 0) AS total_revenue
                        FROM raw_daily_store_item
                        WHERE {where_clause}
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
        except Exception:
            return 0.0
        return float((row or {}).get("total_revenue") or 0)

    def _sum_campaign_discount_cost(
        self,
        campaign_code: str,
        store_id: str | None,
    ) -> float:
        if not self.engine or not has_table(self.engine, "raw_daily_store_cpi_tmzon"):
            return 0.0

        discount_expr = " + ".join(
            [f"COALESCE(NULLIF(dc_amt_{hour:02d}, '')::numeric, 0)" for hour in range(24)]
        )
        clauses = ["cpi_cd = :campaign_code"]
        params: dict[str, object] = {"campaign_code": campaign_code}
        if store_id:
            clauses.append("masked_stor_cd = :store_id")
            params["store_id"] = store_id

        where_clause = " AND ".join(clauses)
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        f"""
                    SELECT COALESCE(SUM({discount_expr}), 0) AS total_discount_cost
                    FROM raw_daily_store_cpi_tmzon
                    WHERE {where_clause}
                    """
                    ),
                    params,
                )
                .mappings()
                .first()
            )
        return float((row or {}).get("total_discount_cost") or 0)

    async def get_campaign_effect(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        campaign_context = self._fetch_campaign_context()
        if not campaign_context:
            raise LookupError("캠페인 효과 실데이터가 없습니다.")

        campaign_code = self._first_text(campaign_context.get("campaign_cd"))
        campaign_name = self._first_text(
            campaign_context.get("campaign_name"), campaign_code, "대표 캠페인"
        )
        benefit_type = self._first_text(campaign_context.get("benefit_type"), "캠페인")
        item_group_count = int(campaign_context.get("group_count") or 0)
        item_count = int(campaign_context.get("item_count") or 0)

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
            campaign_start = campaign_start or self._to_date(
                period_tokens[0] if period_tokens else None
            )
            campaign_end = campaign_end or self._to_date(
                period_tokens[-1] if period_tokens else None
            )
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
        roi_pct = (
            round(((uplift_revenue - discount_cost) / discount_cost) * 100, 1)
            if discount_cost > 0
            else 0.0
        )
        daily_uplift = uplift_revenue / max(period_days, 1)
        payback_days = (
            round(discount_cost / daily_uplift, 1)
            if daily_uplift > 0 and discount_cost > 0
            else None
        )

        return {
            "campaign_code": campaign_code,
            "campaign_name": campaign_name,
            "benefit_type": benefit_type,
            "item_group_count": item_group_count,
            "item_count": item_count,
            "discount_cost": discount_cost,
            "uplift_revenue": uplift_revenue,
            "roi_pct": roi_pct,
            "payback_days": payback_days,
            "periods": [
                {
                    "label": "캠페인 전",
                    "start_date": self._fmt_date(pre_start),
                    "end_date": self._fmt_date(pre_end),
                    "revenue": pre_revenue,
                },
                {
                    "label": "캠페인 중",
                    "start_date": self._fmt_date(during_start),
                    "end_date": self._fmt_date(during_end),
                    "revenue": during_revenue,
                },
                {
                    "label": "캠페인 후",
                    "start_date": self._fmt_date(post_start),
                    "end_date": self._fmt_date(post_end),
                    "revenue": post_revenue,
                },
            ],
        }
