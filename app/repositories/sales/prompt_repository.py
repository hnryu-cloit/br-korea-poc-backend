from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table


class PromptRepositoryMixin:
    engine: Engine | None
    async def list_prompts(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        if not self.engine:
            return []

        top_item_name = self._fetch_top_item_name(store_id) if store_id else None
        top_channel_name = self._fetch_top_channel_name(store_id) if store_id else None
        campaign_context = self._fetch_campaign_context()
        reference_date = self._resolve_payment_reference_date(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        discount_context = self._fetch_discount_program_context(reference_date)
        anomaly_context = self._fetch_prompt_anomaly_context(
            store_id=store_id, date_from=date_from, date_to=date_to
        )

        prompts: list[dict] = []
        if top_item_name:
            prompts.append(
                {
                    "label": f"{top_item_name} 매출이 왜 높죠?",
                    "category": "상품",
                    "prompt": f"최근 {top_item_name} 매출이 높은 이유와 유지 전략을 알려줘",
                }
            )
            prompts.append(
                {
                    "label": f"{top_item_name} 수익 개선",
                    "category": "수익",
                    "prompt": f"{top_item_name} 판매량은 유지하면서 순이익을 높일 실행안을 제안해줘",
                }
            )

        if top_channel_name:
            prompts.append(
                {
                    "label": f"{top_channel_name} 채널 개선",
                    "category": "채널",
                    "prompt": f"{top_channel_name} 채널 매출을 2주 내 개선할 액션을 알려줘",
                }
            )

        if store_id:
            prompts.append(
                {
                    "label": "우리 매장 손익 요약",
                    "category": "매출",
                    "prompt": f"{store_id} 점포의 최근 손익 구조를 요약하고 위험 요인을 알려줘",
                }
            )
            prompts.append(
                {
                    "label": "이번 주 집중 액션",
                    "category": "운영",
                    "prompt": f"{store_id} 점포 기준 이번 주 가장 우선순위가 높은 매출 개선 액션 3가지를 알려줘",
                }
            )

        if campaign_context:
            prompts.append(
                {
                    "label": f"{campaign_context['campaign_name']} 효과 분석",
                    "category": "캠페인",
                    "prompt": (
                        f"{campaign_context['campaign_name']}의 캠페인 전/중/후 매출을 비교하고 "
                        "업리프트·ROI·회수기간을 계산해줘."
                    ),
                }
            )

        if discount_context and self._first_text(discount_context.get("top_telecom_name")):
            prompts.append(
                {
                    "label": "제휴 할인 손익 점검",
                    "category": "할인",
                    "prompt": (
                        f"{self._first_text(discount_context.get('top_telecom_name'))} 제휴 할인의 "
                        "객단가/마진 효과를 분석하고 유지 여부를 추천해줘."
                    ),
                }
            )

        if anomaly_context:
            drop_channel = anomaly_context.get("drop_channel")
            drop_pct = anomaly_context.get("drop_pct")
            if drop_channel and drop_pct is not None and drop_pct <= -10:
                prompts.append(
                    {
                        "label": f"{drop_channel} 급감 원인",
                        "category": "이상징후",
                        "prompt": (
                            f"{drop_channel} 매출이 직전 동기간 대비 {abs(int(round(drop_pct)))}% 감소한 원인을 분석하고 "
                            "즉시 실행할 대응 액션을 제안해줘."
                        ),
                    }
                )
            top_risk_item = anomaly_context.get("top_risk_item")
            if top_risk_item:
                prompts.append(
                    {
                        "label": f"{top_risk_item} 운영 리스크",
                        "category": "이상징후",
                        "prompt": f"{top_risk_item}의 최근 판매 변동성과 품절/과생산 리스크를 진단해줘.",
                    }
                )

        # 카테고리 중복 제거 후 최대 10개 유지
        seen: set[str] = set()
        deduped: list[dict] = []
        for prompt in prompts:
            key = f"{prompt['label']}|{prompt['prompt']}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(prompt)
            if len(deduped) >= 10:
                break
        fallback_pool = self._build_contextual_fallback_prompts(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        if not deduped:
            deduped = list(fallback_pool)
            seen = {f"{prompt['label']}|{prompt['prompt']}" for prompt in deduped}

        # 테스트/계약 호환: 추천 질문은 최소 10개까지 보강한다.
        if len(deduped) < 10:
            extra_templates = [
                {
                    "label": "피크시간 운영 액션",
                    "category": "운영",
                    "prompt": f"{store_id or '현재 점포'}의 피크 시간대 인력/생산 최적화 액션을 제안해줘.",
                },
                {
                    "label": "품절 리스크 점검",
                    "category": "생산",
                    "prompt": f"{store_id or '현재 점포'}의 품절 가능 SKU와 선제 대응 방안을 알려줘.",
                },
                {
                    "label": "할인 정책 점검",
                    "category": "할인",
                    "prompt": f"{store_id or '현재 점포'}의 할인/쿠폰 정책이 마진에 미치는 영향을 점검해줘.",
                },
                {
                    "label": "주문 마감 전 체크",
                    "category": "주문",
                    "prompt": f"{store_id or '현재 점포'} 기준 주문 마감 전 확인해야 할 핵심 항목을 정리해줘.",
                },
                {
                    "label": "메뉴 믹스 개선",
                    "category": "상품",
                    "prompt": f"{store_id or '현재 점포'}의 메뉴 믹스를 개선해 객단가를 높일 방법을 제안해줘.",
                },
                {
                    "label": "주간 성과 요약",
                    "category": "매출",
                    "prompt": f"{store_id or '현재 점포'}의 최근 7일 매출 성과와 다음 주 우선 액션을 요약해줘.",
                },
            ]
            for prompt in [*fallback_pool, *extra_templates]:
                key = f"{prompt['label']}|{prompt['prompt']}"
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(prompt)
                if len(deduped) >= 10:
                    break
        return deduped

    @staticmethod
    def _build_prompt_period_phrase(date_from: str | None, date_to: str | None) -> str:
        if date_from and date_to:
            return f"{date_from}~{date_to}"
        if date_from:
            return f"{date_from} 이후"
        if date_to:
            return f"{date_to}까지"
        return "최근 2주"

    def _build_contextual_fallback_prompts(
        self,
        store_id: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> list[dict]:
        period_text = self._build_prompt_period_phrase(date_from=date_from, date_to=date_to)
        store_text = f"{store_id} 점포" if store_id else "현재 점포"
        return [
            {
                "label": "기간 매출 변동 요인",
                "category": "매출",
                "prompt": f"{store_text}의 {period_text} 매출 변동 요인을 요약해줘.",
            },
            {
                "label": "채널/결제 수익성 점검",
                "category": "채널",
                "prompt": f"{store_text}의 {period_text} 채널/결제수단별 순이익 기여도를 비교해줘.",
            },
            {
                "label": "실행 액션 추천",
                "category": "운영",
                "prompt": f"{store_text}의 {period_text} 데이터를 기준으로 바로 실행할 액션 3가지를 추천해줘.",
            },
        ]

    def _fetch_top_item_name(self, store_id: str) -> str | None:
        if not self.engine:
            return None
        try:
            with self.engine.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            """
                        SELECT item_nm
                        FROM raw_daily_store_item
                        WHERE masked_stor_cd = :store_id
                          AND item_nm IS NOT NULL
                          AND item_nm <> ''
                        GROUP BY item_nm
                        ORDER BY SUM(CAST(COALESCE(sale_amt, '0') AS NUMERIC)) DESC
                        LIMIT 1
                        """
                        ),
                        {"store_id": store_id},
                    )
                    .mappings()
                    .first()
                )
                if row and row.get("item_nm"):
                    return str(row["item_nm"])
        except SQLAlchemyError:
            return None
        return None

    def _fetch_top_channel_name(self, store_id: str) -> str | None:
        if not self.engine:
            return None
        try:
            with self.engine.connect() as connection:
                if has_table(self.engine, "raw_pay_cd"):
                    row = (
                        connection.execute(
                            text(
                                """
                            SELECT COALESCE(pcd.pay_dc_nm, p.pay_way_cd) AS channel_name
                            FROM raw_daily_store_pay_way p
                            LEFT JOIN raw_pay_cd pcd
                              ON p.pay_dtl_cd = pcd.pay_dc_cd
                            WHERE p.masked_stor_cd = :store_id
                            GROUP BY COALESCE(pcd.pay_dc_nm, p.pay_way_cd)
                            ORDER BY SUM(CAST(COALESCE(p.pay_amt, '0') AS NUMERIC)) DESC
                            LIMIT 1
                            """
                            ),
                            {"store_id": store_id},
                        )
                        .mappings()
                        .first()
                    )
                else:
                    row = (
                        connection.execute(
                            text(
                                """
                            SELECT pay_way_cd AS channel_name
                            FROM raw_daily_store_pay_way
                            WHERE masked_stor_cd = :store_id
                            GROUP BY pay_way_cd
                            ORDER BY SUM(CAST(COALESCE(pay_amt, '0') AS NUMERIC)) DESC
                            LIMIT 1
                            """
                            ),
                            {"store_id": store_id},
                        )
                        .mappings()
                        .first()
                    )
                if row and row.get("channel_name"):
                    return str(row["channel_name"])
        except SQLAlchemyError:
            return None
        return None

    @staticmethod
    def _is_campaign_prompt(prompt: str) -> bool:
        return any(keyword in prompt for keyword in ("캠페인", "행사", "프로모션", "시즌", "쿠폰"))

    @staticmethod
    def _format_campaign_date(value: str | None) -> str:
        if not value:
            return "-"
        text_value = str(value)
        if len(text_value) == 8 and text_value.isdigit():
            return f"{text_value[:4]}-{text_value[4:6]}-{text_value[6:8]}"
        return text_value

    @staticmethod
    def _format_campaign_period(start_value: str | None, end_value: str | None) -> str:
        start_text = PromptRepositoryMixin._format_campaign_date(start_value)
        end_text = PromptRepositoryMixin._format_campaign_date(end_value)
        if start_text == "-" and end_text == "-":
            return "기간 정보 없음"
        if start_text == "-":
            return f"{end_text} 종료"
        if end_text == "-":
            return f"{start_text} ~ 종료일 미상"
        return f"{start_text} ~ {end_text}"

    @staticmethod
    def _has_text(value: object) -> bool:
        return value is not None and str(value).strip() != ""

    @staticmethod
    def _first_text(*values: object) -> str:
        for value in values:
            if PromptRepositoryMixin._has_text(value):
                return str(value).strip()
        return ""

    @staticmethod
    def _normalize_date(value: str | None) -> str:
        if not value:
            return date.today().strftime("%Y%m%d")
        return value.replace("-", "")

    def _load_campaign_relation_rows(self, relation_name: str, sheet_name: str) -> list[dict]:
        if self.engine and has_table(self.engine, relation_name):
            try:
                with self.engine.connect() as connection:
                    rows = (
                        connection.execute(text(f"SELECT * FROM {relation_name}")).mappings().all()
                    )
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                pass

        if not self.engine or not has_table(self.engine, "raw_workbook_rows"):
            return []
        if sheet_name in self._workbook_sheet_cache:
            return self._workbook_sheet_cache[sheet_name]

        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            """
                        SELECT row_index, row_values_json
                        FROM raw_workbook_rows
                        WHERE workbook_name = '캠페인+마스터.xlsx'
                          AND sheet_name = :sheet_name
                        ORDER BY row_index
                        """
                        ),
                        {"sheet_name": sheet_name},
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return []

        if not rows:
            self._workbook_sheet_cache[sheet_name] = []
            return []

        headers = rows[0]["row_values_json"] or []
        parsed_rows: list[dict] = []
        for row in rows[1:]:
            values = row["row_values_json"] or []
            if not isinstance(headers, list) or not isinstance(values, list):
                continue
            parsed_row = {
                str(header): values[index]
                for index, header in enumerate(headers)
                if header not in (None, "")
                and index < len(values)
                and values[index] not in (None, "")
            }
            if parsed_row:
                parsed_rows.append(parsed_row)

        self._workbook_sheet_cache[sheet_name] = parsed_rows
        return parsed_rows

    def _fetch_campaign_context(self) -> dict | None:
        campaigns = self._load_campaign_relation_rows("raw_campaign_master", "CPI_MST")
        if not campaigns:
            return None

        group_rows = self._load_campaign_relation_rows(
            "raw_campaign_item_group", "CPI_ITEM_GRP_MNG"
        )
        item_rows = self._load_campaign_relation_rows("raw_campaign_item", "CPI_ITEM_MNG")

        group_count_by_cd: dict[str, int] = {}
        for row in group_rows:
            campaign_cd = self._first_text(row.get("CPI_CD"))
            if not campaign_cd:
                continue
            group_count_by_cd[campaign_cd] = group_count_by_cd.get(campaign_cd, 0) + 1

        item_count_by_cd: dict[str, int] = {}
        for row in item_rows:
            campaign_cd = self._first_text(row.get("CPI_CD"))
            if not campaign_cd:
                continue
            item_count_by_cd[campaign_cd] = item_count_by_cd.get(campaign_cd, 0) + 1

        def _sort_key(row: dict) -> tuple[int, int, int, int, int, int, str, str]:
            active_flag = (
                1
                if (
                    (self._has_text(row.get("USE_YN")) and str(row.get("USE_YN")) == "1")
                    or row.get("USE_YN_NM") == "사용"
                )
                else 0
            )
            try:
                priority = int(float(str(row.get("PRRTY") or "999999")))
            except ValueError:
                priority = 999999
            campaign_cd = self._first_text(row.get("CPI_CD"))
            campaign_name = self._first_text(row.get("CPI_NM"), row.get("CPI_INFO"))
            name_score = (
                6
                if self._has_text(row.get("CPI_NM"))
                else 4 if self._has_text(row.get("CPI_INFO")) else 0
            )
            period_score = (4 if self._has_text(row.get("START_DT")) else 0) + (
                4 if self._has_text(row.get("FNSH_DT")) else 0
            )
            detail_score = (
                2
                if self._has_text(row.get("CPI_CUST_BNFT_TYPE_NM") or row.get("CPI_CUST_BNFT_TYPE"))
                else 0
            )
            detail_score += min(group_count_by_cd.get(campaign_cd, 0), 3)
            detail_score += min(item_count_by_cd.get(campaign_cd, 0), 5)
            completeness_score = (
                int(self._has_text(campaign_cd)) + name_score + period_score + detail_score
            )
            return (
                completeness_score,
                active_flag,
                period_score,
                detail_score,
                -priority,
                str(row.get("START_DT") or ""),
                campaign_name,
            )

        campaigns_sorted = sorted(campaigns, key=_sort_key, reverse=True)
        active_campaigns = [
            row
            for row in campaigns_sorted
            if str(row.get("USE_YN")) == "1" or row.get("USE_YN_NM") == "사용"
        ]
        main_campaign = campaigns_sorted[0]
        campaign_cd = self._first_text(main_campaign.get("CPI_CD"))
        campaign_name = self._first_text(
            main_campaign.get("CPI_NM"), main_campaign.get("CPI_INFO"), campaign_cd, "대표 캠페인"
        )
        campaign_period = self._format_campaign_period(
            main_campaign.get("START_DT"), main_campaign.get("FNSH_DT")
        )
        benefit_type = self._first_text(
            main_campaign.get("CPI_CUST_BNFT_TYPE_NM"),
            main_campaign.get("CPI_CUST_BNFT_TYPE"),
            "캠페인",
        )
        related_groups = [
            row for row in group_rows if self._first_text(row.get("CPI_CD")) == campaign_cd
        ]
        related_items = [
            row for row in item_rows if self._first_text(row.get("CPI_CD")) == campaign_cd
        ]
        group_names = [
            str(row.get("CPI_ITEM_GRP_NM") or "")
            for row in related_groups
            if row.get("CPI_ITEM_GRP_NM")
        ]
        item_names = [str(row.get("ITEM_CD") or "") for row in related_items if row.get("ITEM_CD")]

        return {
            "campaign_cd": campaign_cd,
            "campaign_name": campaign_name,
            "campaign_period": campaign_period,
            "benefit_type": benefit_type,
            "campaign_count": len(active_campaigns) or len(campaigns_sorted),
            "group_count": len(related_groups),
            "item_count": len(related_items),
            "group_names": group_names[:2],
            "item_names": item_names[:2],
        }

    def _fetch_discount_program_context(self, reference_date: str | None) -> dict | None:
        if not self.engine:
            return None
        if not has_table(self.engine, "raw_settlement_master") and not has_table(
            self.engine, "raw_telecom_discount_policy"
        ):
            return None

        target_date = self._normalize_date(reference_date)
        context = {
            "active_settlement_count": 0,
            "rate_program_count": 0,
            "amount_program_count": 0,
            "top_settlement_name": "",
            "top_settlement_method": "",
            "top_telecom_name": "",
            "top_telecom_func": "",
            "top_telecom_target": "",
            "top_telecom_item_count": 0,
        }

        with self.engine.connect() as connection:
            if has_table(self.engine, "raw_settlement_master"):
                settlement = (
                    connection.execute(
                        text(
                            """
                        WITH active_settlement AS (
                            SELECT
                                pay_dc_ty_cd_nm,
                                pay_dc_methd_nm,
                                COUNT(*) AS row_count
                            FROM raw_settlement_master
                            WHERE COALESCE(use_yn, '0') = '1'
                              AND COALESCE(start_dt, '00000000') <= :target_date
                              AND COALESCE(fnsh_dt, '99999999') >= :target_date
                            GROUP BY pay_dc_ty_cd_nm, pay_dc_methd_nm
                        )
                        SELECT
                            COALESCE(SUM(row_count), 0) AS active_settlement_count,
                            COALESCE(SUM(CASE WHEN pay_dc_methd_nm = '율' THEN row_count ELSE 0 END), 0) AS rate_program_count,
                            COALESCE(SUM(CASE WHEN pay_dc_methd_nm = '금액' THEN row_count ELSE 0 END), 0) AS amount_program_count,
                            COALESCE(
                                (
                                    SELECT pay_dc_ty_cd_nm
                                    FROM active_settlement
                                    ORDER BY row_count DESC, pay_dc_ty_cd_nm
                                    LIMIT 1
                                ),
                                ''
                            ) AS top_settlement_name,
                            COALESCE(
                                (
                                    SELECT pay_dc_methd_nm
                                    FROM active_settlement
                                    ORDER BY row_count DESC, pay_dc_ty_cd_nm
                                    LIMIT 1
                                ),
                                ''
                            ) AS top_settlement_method
                        FROM active_settlement
                        """
                        ),
                        {"target_date": target_date},
                    )
                    .mappings()
                    .one()
                )
                context.update({key: settlement[key] for key in settlement.keys()})

            if has_table(self.engine, "raw_telecom_discount_policy"):
                telecom = (
                    connection.execute(
                        text(
                            """
                        WITH active_policy AS (
                            SELECT
                                p.pay_dc_nm,
                                p.func_id_nm,
                                p.dc_apply_trgt_nm,
                                COUNT(*) AS row_count,
                                COALESCE(MAX(item_count.item_count), 0) AS item_count
                            FROM raw_telecom_discount_policy p
                            LEFT JOIN (
                                SELECT
                                    pay_dc_cd,
                                    COUNT(*) AS item_count
                                FROM raw_telecom_discount_item
                                WHERE COALESCE(use_yn, '1') = '1'
                                GROUP BY pay_dc_cd
                            ) item_count
                              ON p.pay_dc_cd = item_count.pay_dc_cd
                            WHERE COALESCE(p.use_yn, '0') = '1'
                              AND COALESCE(p.start_dt, '00000000') <= :target_date
                              AND COALESCE(p.fnsh_dt, '99999999') >= :target_date
                            GROUP BY p.pay_dc_nm, p.func_id_nm, p.dc_apply_trgt_nm
                        )
                        SELECT
                            COALESCE(
                                (
                                    SELECT pay_dc_nm
                                    FROM active_policy
                                    ORDER BY row_count DESC, pay_dc_nm
                                    LIMIT 1
                                ),
                                ''
                            ) AS top_telecom_name,
                            COALESCE(
                                (
                                    SELECT func_id_nm
                                    FROM active_policy
                                    ORDER BY row_count DESC, pay_dc_nm
                                    LIMIT 1
                                ),
                                ''
                            ) AS top_telecom_func,
                            COALESCE(
                                (
                                    SELECT dc_apply_trgt_nm
                                    FROM active_policy
                                    ORDER BY row_count DESC, pay_dc_nm
                                    LIMIT 1
                                ),
                                ''
                            ) AS top_telecom_target,
                            COALESCE(
                                (
                                    SELECT item_count
                                    FROM active_policy
                                    ORDER BY row_count DESC, pay_dc_nm
                                    LIMIT 1
                                ),
                                0
                            ) AS top_telecom_item_count
                        """
                        ),
                        {"target_date": target_date},
                    )
                    .mappings()
                    .one()
                )
                context.update({key: telecom[key] for key in telecom.keys()})

        if not any(context.values()):
            return None
        return context

    def _fetch_prompt_anomaly_context(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict | None:
        if not self.engine:
            return None

        context: dict[str, object] = {}

        where_clause, params = self._build_filters(
            "masked_stor_cd", "sale_dt", store_id, date_from, date_to
        )
        try:
            with self.engine.connect() as connection:
                risk_row = (
                    connection.execute(
                        text(
                            f"""
                        WITH item_daily AS (
                            SELECT
                                item_nm,
                                sale_dt,
                                SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)) AS sale_amt
                            FROM raw_daily_store_item
                            {where_clause}
                            GROUP BY item_nm, sale_dt
                        ),
                        ranked AS (
                            SELECT
                                item_nm,
                                sale_dt,
                                sale_amt,
                                ROW_NUMBER() OVER (PARTITION BY item_nm ORDER BY sale_dt DESC) AS rn
                            FROM item_daily
                        ),
                        item_compare AS (
                            SELECT
                                item_nm,
                                COALESCE(SUM(CASE WHEN rn <= 7 THEN sale_amt END), 0) AS recent_amt,
                                COALESCE(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN sale_amt END), 0) AS prior_amt
                            FROM ranked
                            GROUP BY item_nm
                        )
                        SELECT item_nm
                        FROM item_compare
                        WHERE prior_amt > 0
                        ORDER BY ((recent_amt - prior_amt) / prior_amt) ASC
                        LIMIT 1
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .first()
                )
            if risk_row and risk_row.get("item_nm"):
                context["top_risk_item"] = str(risk_row["item_nm"])
        except SQLAlchemyError:
            pass

        return context or None

    def _resolve_payment_reference_date(
        self,
        store_id: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> str | None:
        if date_to:
            return self._normalize_date(date_to)
        if not self.engine:
            return None

        where_clause, params = self._build_filters(
            "masked_stor_cd", "sale_dt", store_id, date_from, date_to
        )
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        f"""
                    SELECT MAX(sale_dt) AS max_sale_dt
                    FROM raw_daily_store_pay_way
                    {where_clause}
                    """
                    ),
                    params,
                )
                .mappings()
                .first()
            )
        if not row or not row["max_sale_dt"]:
            return None
        return str(row["max_sale_dt"])

    async def get_query_response(self, prompt: str) -> dict:
        source_relation = "raw_daily_store_channel" if self.engine else None

        campaign_context = self._fetch_campaign_context()
        if campaign_context and self._is_campaign_prompt(prompt):
            return {
                "text": (
                    f"캠페인 마스터 기준으로 사용 중인 캠페인 {campaign_context['campaign_count']}건을 확인했습니다. "
                    f"대표 캠페인 {campaign_context['campaign_name']}은 {campaign_context['campaign_period']} 기간의 "
                    f"{campaign_context['benefit_type']} 프로모션입니다. 캠페인 대상 상품군과 매출 지표를 함께 보면 "
                    "행사 영향 해석이 더 명확해집니다."
                ),
                "evidence": [
                    f"대표 캠페인 {campaign_context['campaign_name']} ({campaign_context['campaign_period']})",
                    f"캠페인 상품 그룹 {campaign_context['group_count']}개",
                    f"캠페인 대상 상품 {campaign_context['item_count']}개",
                ],
                "actions": [
                    "캠페인 대상 상품군의 시간대별 매출 변화를 함께 확인",
                    "행사 기간과 비행사 기간의 채널 믹스를 비교",
                    "결제수단별 반응을 함께 점검해 프로모션 효율 확인",
                ],
                "visual_data": {
                    "labels": ["캠페인 수", "그룹 수", "대상 상품 수"],
                    "datasets": [
                        {
                            "label": "현재",
                            "data": [
                                int(campaign_context.get("campaign_count") or 0),
                                int(campaign_context.get("group_count") or 0),
                                int(campaign_context.get("item_count") or 0),
                            ],
                        },
                        {"label": "비교", "data": [0, 0, 0]},
                    ],
                },
            }

        # C-05: 전년 동월 매출 비교
        if self.engine and any(kw in prompt for kw in ("전년", "작년", "동월", "지난해")):
            yoy = self._query_yoy_comparison()
            if yoy:
                return yoy

        # C-08: 상품별 매출 비교
        if self.engine and any(kw in prompt for kw in ("상품", "제품", "품목")):
            item = self._query_item_ranking()
            if item:
                return item

        # C-10: 점포 간 평균 매출 비교
        if self.engine and any(kw in prompt for kw in ("점포", "가맹점", "평균 매출")):
            store = self._query_store_context()
            if store:
                return store

        if self.engine and source_relation:
            try:
                with self.engine.connect() as connection:
                    if "배달 건수" in prompt or "배달" in prompt:
                        summary = (
                            connection.execute(
                                text(
                                    f"""
                                WITH daily AS (
                                    SELECT
                                        sale_dt,
                                        SUM(CAST(COALESCE(NULLIF(CAST(ord_cnt AS TEXT), ''), '0') AS NUMERIC)) AS ord_cnt,
                                        SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)) AS sale_amt
                                    FROM {source_relation}
                                    WHERE ho_chnl_div LIKE '온라인%'
                                    GROUP BY sale_dt
                                    ORDER BY sale_dt DESC
                                    LIMIT 14
                                )
                                SELECT
                                    COALESCE(SUM(CASE WHEN rn <= 7 THEN ord_cnt END), 0) AS recent_orders,
                                    COALESCE(SUM(CASE WHEN rn > 7 THEN ord_cnt END), 0) AS prior_orders,
                                    COALESCE(SUM(CASE WHEN rn <= 7 THEN sale_amt END), 0) AS recent_sales,
                                    COALESCE(SUM(CASE WHEN rn > 7 THEN sale_amt END), 0) AS prior_sales
                                FROM (
                                    SELECT sale_dt, ord_cnt, sale_amt, ROW_NUMBER() OVER (ORDER BY sale_dt DESC) AS rn
                                    FROM daily
                                ) ranked
                                """
                                )
                            )
                            .mappings()
                            .first()
                        )
                        if summary:
                            recent_orders = float(summary["recent_orders"] or 0)
                            prior_orders = float(summary["prior_orders"] or 0)
                            change_pct = (
                                0.0
                                if prior_orders == 0
                                else round(((recent_orders - prior_orders) / prior_orders) * 100, 1)
                            )
                            return {
                                "text": f"최근 1주 온라인 주문은 직전 1주 대비 {change_pct}% 변화했습니다. 주문 수와 채널 매출을 함께 확인해 원인을 점검하는 것이 좋습니다.",
                                "evidence": [
                                    f"최근 1주 온라인 주문 {int(recent_orders)}건",
                                    f"직전 1주 온라인 주문 {int(prior_orders)}건",
                                    f"최근 1주 온라인 매출 {int(float(summary['recent_sales'] or 0)):,}원",
                                ],
                                "actions": [
                                    "온라인 채널별 주문 수 변동을 추가 확인",
                                    "프로모션/노출 변화 여부 점검",
                                    "배달과 픽업 채널을 분리해 재분석",
                                ],
                                "visual_data": {
                                    "labels": ["온라인 주문", "온라인 매출"],
                                    "datasets": [
                                        {
                                            "label": "최근 1주",
                                            "data": [
                                                int(recent_orders),
                                                int(float(summary["recent_sales"] or 0)),
                                            ],
                                        },
                                        {
                                            "label": "직전 1주",
                                            "data": [
                                                int(prior_orders),
                                                int(float(summary["prior_sales"] or 0)),
                                            ],
                                        },
                                    ],
                                },
                            }
                    # C-09: 채널별 매출 상세 분석
                    elif any(kw in prompt for kw in ("채널", "쿠팡", "배민", "해피오더")):
                        channel = self._query_channel_breakdown(connection, source_relation)
                        if channel:
                            return channel
            except SQLAlchemyError:
                pass
        return {
            "text": "해당 질의에 대한 데이터를 조회하지 못했습니다. 데이터 적재 상태를 확인하거나 질문을 다시 시도해 주세요.",
            "evidence": [],
            "actions": [],
        }

    def _query_yoy_comparison(self) -> dict | None:
        """전년 동월 대비 이번 달 매출 비교 (C-05)"""
        if not self.engine:
            return None
        relation, amt_col = "raw_daily_store_item", "sale_amt"
        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            SUBSTRING(CAST(sale_dt AS TEXT), 1, 4) AS yr,
                            SUBSTRING(CAST(sale_dt AS TEXT), 5, 2) AS mo,
                            SUM(CAST(COALESCE(NULLIF(CAST({amt_col} AS TEXT), ''), '0') AS NUMERIC)) AS total_amt
                        FROM {relation}
                        WHERE sale_dt IS NOT NULL
                        GROUP BY yr, mo
                        ORDER BY yr DESC, mo DESC
                        LIMIT 24
                        """
                        )
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return None

        if not rows:
            return None

        by_ym: dict[tuple[str, str], float] = {}
        for row in rows:
            yr, mo = str(row["yr"]).strip(), str(row["mo"]).strip()
            if len(yr) == 4 and len(mo) == 2:
                by_ym[(yr, mo)] = float(row["total_amt"] or 0)

        sorted_ym = sorted(by_ym.keys(), reverse=True)
        if not sorted_ym:
            return None

        latest_yr, latest_mo = sorted_ym[0]
        prev_yr = str(int(latest_yr) - 1)
        current_amt = by_ym.get((latest_yr, latest_mo), 0)
        prior_amt = by_ym.get((prev_yr, latest_mo), 0)

        if prior_amt > 0:
            change_pct = round((current_amt - prior_amt) / prior_amt * 100, 1)
            direction = "증가" if change_pct > 0 else "감소"
            return {
                "text": (
                    f"{latest_yr}년 {int(latest_mo)}월 매출은 전년 동월({prev_yr}년 {int(latest_mo)}월) 대비 "
                    f"{abs(change_pct)}% {direction}했습니다."
                ),
                "evidence": [
                    f"{latest_yr}년 {int(latest_mo)}월 매출: {int(current_amt):,}원",
                    f"{prev_yr}년 {int(latest_mo)}월 매출: {int(prior_amt):,}원",
                    f"전년 동월 대비 {change_pct:+.1f}%",
                ],
                "actions": [
                    "월별 매출 추이를 인사이트 탭에서 추가 확인해 주세요.",
                    "전년 동기 대비 성장 요인을 채널 믹스와 함께 점검해 주세요.",
                ],
                "visual_data": {
                    "labels": [f"{int(latest_mo)}월 매출"],
                    "datasets": [
                        {"label": f"{latest_yr}년", "data": [int(current_amt)]},
                        {"label": f"{prev_yr}년", "data": [int(prior_amt)]},
                    ],
                },
            }
        return {
            "text": f"{latest_yr}년 {int(latest_mo)}월 매출 데이터를 확인했습니다. 전년 동월 비교 데이터가 부족합니다.",
            "evidence": [
                f"{latest_yr}년 {int(latest_mo)}월 누적 매출: {int(current_amt):,}원",
                "전년 동월 데이터 없음",
            ],
            "actions": ["인사이트 탭에서 최신 기간별 매출 추이를 확인해 주세요."],
            "visual_data": {
                "labels": [f"{latest_yr}-{latest_mo}"],
                "datasets": [
                    {"label": "매출", "data": [int(current_amt)]},
                    {"label": "비교", "data": [0]},
                ],
            },
        }

    def _query_item_ranking(self) -> dict | None:
        """상품별 매출 순위 조회 (C-08)"""
        if not self.engine:
            return None
        relation, amt_col = "raw_daily_store_item", "sale_amt"
        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '기타') AS item_nm,
                            SUM(CAST(COALESCE(NULLIF(CAST({amt_col} AS TEXT), ''), '0') AS NUMERIC)) AS total_amt,
                            SUM(CAST(COALESCE(NULLIF(CAST(sale_qty AS TEXT), ''), '0') AS NUMERIC)) AS total_qty
                        FROM {relation}
                        GROUP BY item_nm
                        ORDER BY total_amt DESC, total_qty DESC
                        LIMIT 5
                        """
                        )
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError:
            return None

        if not rows:
            return None

        top_item = str(rows[0]["item_nm"])
        evidence = [
            f"{str(row['item_nm'])}: {int(float(row['total_amt'] or 0)):,}원 / {int(float(row['total_qty'] or 0)):,}개"
            for row in rows[:4]
        ]
        labels = [str(row["item_nm"]) for row in rows[:4]]
        sales_data = [int(float(row["total_amt"] or 0)) for row in rows[:4]]
        if sales_data:
            avg_sales = int(sum(sales_data) / len(sales_data))
        else:
            avg_sales = 0
        return {
            "text": (
                f"상품별 매출 분석 결과, {top_item}이(가) 전체 기간 기준 가장 높은 매출을 기록했습니다. "
                "상위 4개 상품 내역을 아래에 정리했습니다."
            ),
            "evidence": evidence,
            "actions": [
                "상위 상품과 음료 묶음 판매를 강화해 객단가를 높여 주세요.",
                "하위 상품은 비피크 시간대 특가 테스트를 검토해 주세요.",
            ],
            "visual_data": {
                "labels": labels,
                "datasets": [
                    {"label": "상품 매출", "data": sales_data},
                    {"label": "상위 평균 매출", "data": [avg_sales for _ in labels]},
                ],
            },
        }

    def _query_store_context(self) -> dict | None:
        """비교 대상 매장 수 조회 (C-10)"""
        if not has_table(self.engine, "raw_store_master"):
            return None
        try:
            with self.engine.connect() as connection:
                store_count = connection.execute(
                    text("SELECT COUNT(*) AS store_count FROM raw_store_master")
                ).scalar_one()
        except SQLAlchemyError:
            return None

        count = int(store_count or 0)
        if count == 0:
            return None
        return {
            "text": (
                f"현재 비교 가능한 매장은 {count}개입니다. "
                "점포 간 평균 매출 비교는 채널·상품 믹스 데이터를 함께 활용합니다."
            ),
            "evidence": [
                f"비교 대상 매장: {count}개 (raw_store_master 기준)",
                "채널별·상품별 비교는 인사이트 섹션에서 확인 가능",
            ],
            "actions": [
                "인사이트 탭에서 채널 믹스와 메뉴 믹스를 확인해 주세요.",
                "매장 간 상세 비교가 필요하면 본사 계정으로 재시도해 주세요.",
            ],
        }

    def _query_channel_breakdown(self, connection, source_relation: str) -> dict | None:
        """채널별 매출 상세 분석 (C-09)"""
        try:
            rows = (
                connection.execute(
                    text(
                        f"""
                    SELECT
                        COALESCE(NULLIF(ho_chnl_div, ''), '기타') AS channel_div,
                        SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)) AS sale_amt,
                        SUM(CAST(COALESCE(NULLIF(CAST(ord_cnt AS TEXT), ''), '0') AS NUMERIC)) AS ord_cnt
                    FROM {source_relation}
                    GROUP BY channel_div
                    ORDER BY sale_amt DESC
                    LIMIT 5
                    """
                    )
                )
                .mappings()
                .all()
            )
        except SQLAlchemyError:
            return None

        if not rows:
            return None

        total_amt = sum(float(row["sale_amt"] or 0) for row in rows)
        evidence = [
            f"{str(row['channel_div'])}: {int(float(row['sale_amt'] or 0)):,}원 ({int(float(row['ord_cnt'] or 0)):,}건)"
            for row in rows[:3]
        ]
        top_channel = str(rows[0]["channel_div"])
        return {
            "text": (
                f"채널별 매출 분석 결과, {top_channel} 채널 비중이 가장 높습니다. "
                "채널 전환 기회와 집중 시간대를 함께 확인하는 것이 좋습니다."
            ),
            "evidence": evidence,
            "actions": [
                "온라인 강세 시간대에 배달/픽업 전용 구성을 노출해 주세요.",
                "오프라인 집중 시간대에는 회전율 중심 진열 운영을 유지해 주세요.",
                "채널 인사이트 섹션에서 시간대별 온/오프라인 비중을 확인해 주세요.",
            ],
            "comparison": {
                "store": "현재 매장",
                "peer_group": "채널 평균",
                "summary": f"전체 채널 매출 기준 {top_channel} 비중이 최상위입니다.",
                "metrics": [
                    {
                        "label": str(row["channel_div"]),
                        "store_value": (
                            f"{round(float(row['sale_amt'] or 0) / total_amt * 100, 1):.1f}%"
                            if total_amt > 0
                            else "0%"
                        ),
                        "peer_value": "-",
                    }
                    for row in rows[:3]
                ],
            },
            "visual_data": {
                "labels": [str(row["channel_div"]) for row in rows[:3]],
                "datasets": [
                    {
                        "label": "채널 매출",
                        "data": [int(float(row["sale_amt"] or 0)) for row in rows[:3]],
                    },
                    {
                        "label": "채널 평균 매출",
                        "data": (
                            [int(total_amt / len(rows[:3])) for _ in rows[:3]] if rows[:3] else []
                        ),
                    },
                ],
                "channel_analysis": {
                    "online_amt": int(
                        sum(
                            float(row["sale_amt"] or 0)
                            for row in rows
                            if "온라인" in str(row.get("channel_div") or "")
                        )
                    ),
                    "offline_amt": int(
                        sum(
                            float(row["sale_amt"] or 0)
                            for row in rows
                            if "온라인" not in str(row.get("channel_div") or "")
                        )
                    ),
                },
            },
        }
