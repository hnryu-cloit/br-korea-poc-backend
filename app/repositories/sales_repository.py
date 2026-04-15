from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

SUGGESTED_PROMPTS = [
    {"label": "배달 주문이 줄었어요", "category": "배달", "prompt": "이번 주 배달 건수가 지난주보다 줄어든 원인을 알려줘"},
    {"label": "행사 효과가 궁금해요", "category": "캠페인", "prompt": "T-day 행사 이후 매출과 재방문 영향이 어땠는지 분석해줘"},
    {"label": "오전 시간대 매출 비교", "category": "시간대", "prompt": "오전 10시부터 12시까지 채널별 매출 차이를 비교해줘"},
    {"label": "도넛+커피 묶음 늘리는 방법", "category": "상품", "prompt": "도넛과 커피 묶음 판매를 늘리기 위한 액션을 제안해줘"},
    {"label": "작년 같은 달과 비교", "category": "매출", "prompt": "전년 동월 대비 이번 달 매출 차이를 분석해줘"},
    {"label": "쿠폰 효과가 없어진 것 같아요", "category": "마케팅", "prompt": "앱 쿠폰 사용률 하락 원인과 개선 방법을 알려줘"},
    {"label": "점심 배달이 안 들어와요", "category": "운영", "prompt": "점심 시간대 배달 전환율이 낮은 이유를 분석해줘"},
    {"label": "단골 손님이 줄었나요?", "category": "고객", "prompt": "최근 2주간 재방문 고객 비율 변화와 액션을 알려줘"},
    {"label": "배달앱 vs 홀 수익 비교", "category": "수익", "prompt": "배달앱과 홀 채널의 이익률 차이를 비교해줘"},
    {"label": "다음 달 잘 팔릴 상품은?", "category": "상품", "prompt": "다음 달 시즌 수요를 반영한 상품 믹스를 추천해줘"},
]

QUERY_RESPONSES = {
    "이번 주 배달 건수가 지난주보다 줄어든 원인을 알려줘": {
        "text": "이번 주 배달 주문이 지난주보다 14.3% 줄었어요. 가장 큰 이유는 점심 시간에 앱 주문이 덜 들어온 것과 쿠폰 소진 영향입니다.",
        "evidence": ["점심 시간대 배달 주문 21건 감소", "앱 쿠폰 사용률 38% -> 22% 하락", "배달앱 노출 순위 3위 -> 5위"],
        "actions": ["점심 시간대 배달 전용 쿠폰 재발급", "배달앱 광고비 조정 검토", "도넛+음료 배달 특가 테스트"],
    }
}

_DEFAULT_PEAK_HOURS = {
    "title": "시간대 운영 코칭",
    "summary": "점심 전후와 퇴근 시간대 매출 집중 패턴을 기준으로 생산과 진열 우선순위를 안내합니다.",
    "metrics": [
        {"label": "핵심 시간대", "value": "11시", "detail": "대표 상품 글레이즈드"},
        {"label": "보완 시간대", "value": "15시", "detail": "프로모션 점검 필요"},
        {"label": "집중 상품", "value": "오리지널 글레이즈드", "detail": "시간대 매출 상위"},
    ],
    "actions": ["11시 이전 핵심 상품 진열을 완료해 주세요.", "15시 저조 시간대에는 세트/음료 동시 노출을 강화해 주세요."],
    "status": "active",
}

_DEFAULT_CHANNEL_MIX = {
    "title": "채널 전환 인사이트",
    "summary": "오프라인 중심 매출 구조로 보이며 특정 시간대 온라인 전환 보완 여지가 있습니다.",
    "metrics": [
        {"label": "오프라인 비중", "value": "68%", "detail": "매장 방문 중심"},
        {"label": "온라인 비중", "value": "32%", "detail": "점심 시간대 보완 가능"},
        {"label": "온라인 강세 시간대", "value": "12시", "detail": "배달/픽업 집중"},
    ],
    "actions": ["점심 시간대 온라인 전용 구성 노출을 검토해 주세요.", "오프라인 강세 시간대는 회전율 중심 운영을 유지해 주세요."],
    "status": "active",
}

_DEFAULT_PAYMENT_MIX = {
    "title": "결제/할인 민감도",
    "summary": "카드와 간편결제 비중이 높고 할인 의존도는 과도하지 않은 편입니다.",
    "metrics": [
        {"label": "주요 결제수단", "value": "신용카드", "detail": "매출 비중 54%"},
        {"label": "할인 비중", "value": "12%", "detail": "쿠폰/제휴 포함"},
        {"label": "점검 포인트", "value": "간편결제", "detail": "프로모션 연계 여지"},
    ],
    "actions": ["간편결제 프로모션의 추가 유입 효과를 확인해 주세요.", "할인 비중이 높아지는 기간에는 객단가 방어 상품을 함께 제안해 주세요."],
    "status": "normal",
}

_DEFAULT_MENU_MIX = {
    "title": "메뉴 믹스 추천",
    "summary": "상위 상품 집중도가 높아 보완 상품을 동시 진열하면 객단가 개선 여지가 있습니다.",
    "metrics": [
        {"label": "대표 상품", "value": "오리지널 글레이즈드", "detail": "순매출 상위"},
        {"label": "보완 상품", "value": "아메리카노", "detail": "동반 제안 후보"},
        {"label": "운영 포인트", "value": "세트 노출", "detail": "점심 이후 강화"},
    ],
    "actions": ["대표 상품과 음료를 묶어 동시 노출해 주세요.", "저성과 상품은 피크타임보다 비피크 시간대에 테스트해 주세요."],
    "status": "active",
}


class SalesRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine
        self._workbook_sheet_cache: dict[str, list[dict]] = {}

    async def list_prompts(self) -> list[dict]:
        return SUGGESTED_PROMPTS

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
        start_text = SalesRepository._format_campaign_date(start_value)
        end_text = SalesRepository._format_campaign_date(end_value)
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
            if SalesRepository._has_text(value):
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
                    rows = connection.execute(text(f"SELECT * FROM {relation_name}")).mappings().all()
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                pass

        if not self.engine or not has_table(self.engine, "raw_workbook_rows"):
            return []
        if sheet_name in self._workbook_sheet_cache:
            return self._workbook_sheet_cache[sheet_name]

        try:
            with self.engine.connect() as connection:
                rows = connection.execute(
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
                ).mappings().all()
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

        group_rows = self._load_campaign_relation_rows("raw_campaign_item_group", "CPI_ITEM_GRP_MNG")
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
            active_flag = 1 if (
                (self._has_text(row.get("USE_YN")) and str(row.get("USE_YN")) == "1")
                or row.get("USE_YN_NM") == "사용"
            ) else 0
            try:
                priority = int(float(str(row.get("PRRTY") or "999999")))
            except ValueError:
                priority = 999999
            campaign_cd = self._first_text(row.get("CPI_CD"))
            campaign_name = self._first_text(row.get("CPI_NM"), row.get("CPI_INFO"))
            name_score = 6 if self._has_text(row.get("CPI_NM")) else 4 if self._has_text(row.get("CPI_INFO")) else 0
            period_score = (4 if self._has_text(row.get("START_DT")) else 0) + (4 if self._has_text(row.get("FNSH_DT")) else 0)
            detail_score = 2 if self._has_text(row.get("CPI_CUST_BNFT_TYPE_NM") or row.get("CPI_CUST_BNFT_TYPE")) else 0
            detail_score += min(group_count_by_cd.get(campaign_cd, 0), 3)
            detail_score += min(item_count_by_cd.get(campaign_cd, 0), 5)
            completeness_score = (
                int(self._has_text(campaign_cd))
                + name_score
                + period_score
                + detail_score
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
            row for row in campaigns_sorted if str(row.get("USE_YN")) == "1" or row.get("USE_YN_NM") == "사용"
        ]
        main_campaign = campaigns_sorted[0]
        campaign_cd = self._first_text(main_campaign.get("CPI_CD"))
        campaign_name = self._first_text(main_campaign.get("CPI_NM"), main_campaign.get("CPI_INFO"), campaign_cd, "대표 캠페인")
        campaign_period = self._format_campaign_period(main_campaign.get("START_DT"), main_campaign.get("FNSH_DT"))
        benefit_type = self._first_text(
            main_campaign.get("CPI_CUST_BNFT_TYPE_NM"),
            main_campaign.get("CPI_CUST_BNFT_TYPE"),
            "캠페인",
        )
        related_groups = [row for row in group_rows if self._first_text(row.get("CPI_CD")) == campaign_cd]
        related_items = [row for row in item_rows if self._first_text(row.get("CPI_CD")) == campaign_cd]
        group_names = [str(row.get("CPI_ITEM_GRP_NM") or "") for row in related_groups if row.get("CPI_ITEM_GRP_NM")]
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
        if not has_table(self.engine, "raw_settlement_master") and not has_table(self.engine, "raw_telecom_discount_policy"):
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
                settlement = connection.execute(
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
                ).mappings().one()
                context.update({key: settlement[key] for key in settlement.keys()})

            if has_table(self.engine, "raw_telecom_discount_policy"):
                telecom = connection.execute(
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
                ).mappings().one()
                context.update({key: telecom[key] for key in telecom.keys()})

        if not any(context.values()):
            return None
        return context

    def _resolve_payment_reference_date(
        self,
        store_id: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> str | None:
        if date_to:
            return self._normalize_date(date_to)
        if not self.engine or not has_table(self.engine, "raw_daily_store_pay_way"):
            return None

        where_clause, params = self._build_filters("masked_stor_cd", "sale_dt", store_id, date_from, date_to)
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT MAX(sale_dt) AS max_sale_dt
                    FROM raw_daily_store_pay_way
                    {where_clause}
                    """
                ),
                params,
            ).mappings().first()
        if not row or not row["max_sale_dt"]:
            return None
        return str(row["max_sale_dt"])

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

    async def get_query_response(self, prompt: str) -> dict:
        source_relation = None
        if self.engine and has_table(self.engine, "core_channel_sales"):
            source_relation = "core_channel_sales"
        elif self.engine and has_table(self.engine, "raw_daily_store_online"):
            source_relation = "raw_daily_store_online"

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
                        summary = connection.execute(
                            text(
                                f"""
                                WITH daily AS (
                                    SELECT
                                        sale_dt,
                                        SUM(ord_cnt) AS ord_cnt,
                                        SUM(sale_amt) AS sale_amt
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
                        ).mappings().first()
                        if summary:
                            recent_orders = float(summary["recent_orders"] or 0)
                            prior_orders = float(summary["prior_orders"] or 0)
                            change_pct = 0.0 if prior_orders == 0 else round(((recent_orders - prior_orders) / prior_orders) * 100, 1)
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
                            }
                    # C-09: 채널별 매출 상세 분석
                    elif any(kw in prompt for kw in ("채널", "쿠팡", "배민", "해피오더")):
                        channel = self._query_channel_breakdown(connection, source_relation)
                        if channel:
                            return channel
            except SQLAlchemyError:
                pass
        return QUERY_RESPONSES.get(
            prompt,
            {
                "text": "요청하신 내용을 기준으로 비교 분석을 완료했습니다. 주요 근거와 실행 가능한 액션을 아래에 정리했습니다.",
                "evidence": ["관련 기간 데이터 비교 완료", "매장 기준 비교군 계산 완료", "매장 맞춤 분석 적용"],
                "actions": ["점심 시간대 채널 성과 재점검", "쿠폰 정책 재설계 검토"],
            },
        )

    async def get_insights(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        insights = {
            "peak_hours": _DEFAULT_PEAK_HOURS,
            "channel_mix": _DEFAULT_CHANNEL_MIX,
            "payment_mix": _DEFAULT_PAYMENT_MIX,
            "menu_mix": _DEFAULT_MENU_MIX,
            "filtered_store_id": store_id,
            "filtered_date_from": date_from,
            "filtered_date_to": date_to,
        }
        campaign_context = self._fetch_campaign_context()
        if campaign_context:
            insights["campaign_seasonality"] = self._build_campaign_insight(campaign_context)
        if not self.engine:
            return insights

        try:
            if has_table(self.engine, "core_hourly_item_sales"):
                peak_hours = self._fetch_peak_hours_insight(store_id=store_id, date_from=date_from, date_to=date_to)
                if peak_hours:
                    insights["peak_hours"] = peak_hours
            if has_table(self.engine, "core_channel_sales"):
                channel_mix = self._fetch_channel_mix_insight(store_id=store_id, date_from=date_from, date_to=date_to)
                if channel_mix:
                    insights["channel_mix"] = channel_mix
            if (
                has_table(self.engine, "raw_daily_store_pay_way")
                or has_table(self.engine, "raw_settlement_master")
                or has_table(self.engine, "raw_telecom_discount_policy")
            ):
                payment_mix = self._fetch_payment_mix_insight(store_id=store_id, date_from=date_from, date_to=date_to)
                if payment_mix:
                    insights["payment_mix"] = payment_mix
            if has_table(self.engine, "core_daily_item_sales"):
                menu_mix = self._fetch_menu_mix_insight(store_id=store_id, date_from=date_from, date_to=date_to)
                if menu_mix:
                    insights["menu_mix"] = menu_mix
        except SQLAlchemyError:
            pass
        return insights

    def _build_filters(
        self,
        column_store: str,
        column_date: str,
        store_id: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> tuple[str, dict]:
        clauses: list[str] = []
        params: dict[str, str] = {}
        if store_id:
            clauses.append(f"{column_store} = :store_id")
            params["store_id"] = store_id
        if date_from:
            clauses.append(f"{column_date} >= :date_from")
            params["date_from"] = date_from.replace("-", "")
        if date_to:
            clauses.append(f"{column_date} <= :date_to")
            params["date_to"] = date_to.replace("-", "")
        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _fetch_peak_hours_insight(self, store_id: str | None, date_from: str | None, date_to: str | None) -> dict | None:
        where_clause, params = self._build_filters("masked_stor_cd", "sale_dt", store_id, date_from, date_to)
        with self.engine.connect() as connection:
            metrics = connection.execute(
                text(
                    f"""
                    WITH hourly AS (
                        SELECT
                            tmzon_div,
                            SUM(net_sale_amt) AS net_sale_amt,
                            SUM(sale_qty) AS sale_qty
                        FROM core_hourly_item_sales
                        {where_clause}
                        GROUP BY tmzon_div
                    ),
                    top_items AS (
                        SELECT
                            tmzon_div,
                            item_nm,
                            SUM(net_sale_amt) AS net_sale_amt,
                            ROW_NUMBER() OVER (PARTITION BY tmzon_div ORDER BY SUM(net_sale_amt) DESC, item_nm) AS rn
                        FROM core_hourly_item_sales
                        {where_clause}
                        GROUP BY tmzon_div, item_nm
                    )
                    SELECT
                        h.tmzon_div,
                        h.net_sale_amt,
                        h.sale_qty,
                        ti.item_nm
                    FROM hourly h
                    LEFT JOIN top_items ti
                        ON h.tmzon_div = ti.tmzon_div
                       AND ti.rn = 1
                    ORDER BY h.net_sale_amt DESC, h.tmzon_div
                    LIMIT 3
                    """
                ),
                params,
            ).mappings().all()
            slow_slots = connection.execute(
                text(
                    f"""
                    SELECT
                        tmzon_div,
                        SUM(net_sale_amt) AS net_sale_amt
                    FROM core_hourly_item_sales
                    {where_clause}
                    GROUP BY tmzon_div
                    HAVING SUM(net_sale_amt) > 0
                    ORDER BY SUM(net_sale_amt) ASC, tmzon_div
                    LIMIT 2
                    """
                ),
                params,
            ).mappings().all()

        if not metrics:
            return None

        lead = metrics[0]
        summary = (
            f"{int(lead['tmzon_div']):02d}시가 가장 강하고 "
            f"{(lead['item_nm'] or '핵심 상품')} 중심으로 매출이 집중됩니다."
        )
        metric_items = [
            {
                "label": f"{int(metric['tmzon_div']):02d}시",
                "value": f"{int(float(metric['net_sale_amt'] or 0)):,}원",
                "detail": f"대표 상품 {metric['item_nm'] or '-'} / 판매 {int(float(metric['sale_qty'] or 0))}개",
            }
            for metric in metrics[:2]
        ]
        if slow_slots:
            slow = slow_slots[0]
            metric_items.append(
                {
                    "label": "보완 시간대",
                    "value": f"{int(slow['tmzon_div']):02d}시",
                    "detail": f"순매출 {int(float(slow['net_sale_amt'] or 0)):,}원",
                }
            )

        return {
            "title": "시간대 운영 코칭",
            "summary": summary,
            "metrics": metric_items,
            "actions": [
                f"{int(lead['tmzon_div']):02d}시 이전에 핵심 상품 생산·진열을 완료해 주세요.",
                "저조 시간대에는 세트 제안이나 음료 동시 노출을 강화해 주세요.",
            ],
            "status": "active",
        }

    def _fetch_channel_mix_insight(self, store_id: str | None, date_from: str | None, date_to: str | None) -> dict | None:
        where_clause, params = self._build_filters("masked_stor_cd", "sale_dt", store_id, date_from, date_to)
        with self.engine.connect() as connection:
            channel_rows = connection.execute(
                text(
                    f"""
                    SELECT
                        COALESCE(NULLIF(ho_chnl_div, ''), '기타') AS channel_div,
                        SUM(sale_amt) AS sale_amt,
                        SUM(ord_cnt) AS ord_cnt
                    FROM core_channel_sales
                    {where_clause}
                    GROUP BY COALESCE(NULLIF(ho_chnl_div, ''), '기타')
                    ORDER BY SUM(sale_amt) DESC
                    """
                ),
                params,
            ).mappings().all()
            time_rows = connection.execute(
                text(
                    f"""
                    SELECT
                        tmzon_div,
                        SUM(CASE WHEN ho_chnl_div LIKE '온라인%' THEN sale_amt ELSE 0 END) AS online_sale_amt,
                        SUM(CASE WHEN ho_chnl_div LIKE '오프라인%' THEN sale_amt ELSE 0 END) AS offline_sale_amt
                    FROM core_channel_sales
                    {where_clause}
                    GROUP BY tmzon_div
                    ORDER BY online_sale_amt DESC, tmzon_div
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()

        if not channel_rows:
            return None

        total_sales = sum(float(row["sale_amt"] or 0) for row in channel_rows)
        metric_items = []
        for row in channel_rows[:2]:
            ratio = 0 if total_sales == 0 else round(float(row["sale_amt"] or 0) / total_sales * 100, 1)
            metric_items.append(
                {
                    "label": str(row["channel_div"]),
                    "value": f"{ratio:.1f}%",
                    "detail": f"매출 {int(float(row['sale_amt'] or 0)):,}원 / 주문 {int(float(row['ord_cnt'] or 0)):,}건",
                }
            )

        if time_rows:
            online_sale = float(time_rows["online_sale_amt"] or 0)
            offline_sale = float(time_rows["offline_sale_amt"] or 0)
            metric_items.append(
                {
                    "label": "온라인 강세 시간대",
                    "value": f"{int(time_rows['tmzon_div']):02d}시",
                    "detail": f"온라인 {int(online_sale):,}원 / 오프라인 {int(offline_sale):,}원",
                }
            )

        top_channel = channel_rows[0]
        return {
            "title": "채널 전환 인사이트",
            "summary": f"{top_channel['channel_div']} 비중이 가장 높고 채널별 집중 시간대 차이가 보입니다.",
            "metrics": metric_items,
            "actions": [
                "온라인 강세 시간대에는 배달/픽업 전용 구성을 노출해 주세요.",
                "오프라인 강세 시간대에는 회전율 중심으로 진열 우선순위를 유지해 주세요.",
            ],
            "status": "active",
        }

    def _fetch_payment_mix_insight(self, store_id: str | None, date_from: str | None, date_to: str | None) -> dict | None:
        rows: list[dict] = []
        discount_ratio = 0.0
        total_amt = 0.0
        if has_table(self.engine, "raw_daily_store_pay_way"):
            where_clause, params = self._build_filters("masked_stor_cd", "sale_dt", store_id, date_from, date_to)
            with self.engine.connect() as connection:
                rows = [
                    dict(row)
                    for row in connection.execute(
                        text(
                            f"""
                            SELECT
                                COALESCE(NULLIF(pay_way_cd_nm, ''), NULLIF(pay_way_cd, ''), '기타') AS payment_label,
                                COALESCE(NULLIF(pay_way_cd, ''), '기타') AS payment_code,
                                SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) AS payment_amt
                            FROM raw_daily_store_pay_way
                            {where_clause}
                            GROUP BY
                                COALESCE(NULLIF(pay_way_cd_nm, ''), NULLIF(pay_way_cd, ''), '기타'),
                                COALESCE(NULLIF(pay_way_cd, ''), '기타')
                            ORDER BY SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) DESC
                            """
                        ),
                        params,
                    ).mappings().all()
                ]

            total_amt = sum(float(row["payment_amt"] or 0) for row in rows)
            discount_amt = sum(float(row["payment_amt"] or 0) for row in rows if row["payment_code"] in {"03", "19"})
            discount_ratio = 0 if total_amt == 0 else round(discount_amt / total_amt * 100, 1)

        reference_date = self._resolve_payment_reference_date(store_id=store_id, date_from=date_from, date_to=date_to)
        discount_context = self._fetch_discount_program_context(reference_date)
        if not rows and not discount_context:
            return None

        metric_items = []
        if rows:
            for row in rows[:2]:
                ratio = 0 if total_amt == 0 else round(float(row["payment_amt"] or 0) / total_amt * 100, 1)
                metric_items.append(
                    {
                        "label": str(row["payment_label"]),
                        "value": f"{ratio:.1f}%",
                        "detail": f"결제금액 {int(float(row['payment_amt'] or 0)):,}원",
                    }
                )
            metric_items.append(
                {
                    "label": "할인 결제 비중",
                    "value": f"{discount_ratio:.1f}%",
                    "detail": "제휴할인 + 캠페인할인결제 기준",
                }
            )

        actions = [
            "상위 결제수단과 연결된 프로모션 성과를 함께 비교해 주세요.",
            "특정 할인수단 비중이 높아질 때는 객단가 방어 상품을 함께 제안해 주세요.",
        ]
        summary = "결제수단 비중과 할인 프로그램 구성을 함께 점검할 수 있습니다."

        if rows:
            summary = f"{rows[0]['payment_label']} 비중이 가장 높고 결제수단 편중 여부를 운영 관점에서 점검할 수 있습니다."

        if discount_context:
            active_count = int(discount_context.get("active_settlement_count") or 0)
            top_settlement_name = self._first_text(discount_context.get("top_settlement_name"))
            top_settlement_method = self._first_text(discount_context.get("top_settlement_method"))
            top_telecom_name = self._first_text(discount_context.get("top_telecom_name"))
            top_telecom_target = self._first_text(discount_context.get("top_telecom_target"))
            top_telecom_item_count = int(discount_context.get("top_telecom_item_count") or 0)

            if active_count or top_telecom_name:
                summary += " "
                if active_count:
                    summary += f"정산 기준 정보상 현재 활성 할인 기준은 {active_count}건이며 "
                    if top_settlement_name:
                        summary += f"대표 기준은 {top_settlement_name}"
                        if top_settlement_method:
                            summary += f"({top_settlement_method})"
                        summary += "입니다."
                if top_telecom_name:
                    if active_count:
                        summary += " "
                    summary += f"통신사 제휴 정책 기준 대표 프로그램은 {top_telecom_name}"
                    if top_telecom_target:
                        summary += f"({top_telecom_target})"
                    summary += "입니다."

            if top_settlement_name:
                metric_items.append(
                    {
                        "label": "활성 정산 기준",
                        "value": top_settlement_name,
                        "detail": top_settlement_method or "정산 기준 정보 기준",
                    }
                )
            elif active_count:
                metric_items.append(
                    {
                        "label": "활성 정산 기준",
                        "value": f"{active_count}건",
                        "detail": "정산 기준 정보 기준",
                    }
                )

            if top_telecom_name:
                telecom_detail = top_telecom_target or "통신사 제휴 할인"
                if top_telecom_item_count:
                    telecom_detail = f"{telecom_detail} / 대상 상품 {top_telecom_item_count}개"
                metric_items.append(
                    {
                        "label": "대표 제휴 할인",
                        "value": top_telecom_name,
                        "detail": telecom_detail,
                    }
                )
                actions[0] = f"{top_telecom_name} 반응을 상위 결제수단과 함께 비교해 주세요."
                if top_telecom_item_count:
                    actions.append("제휴 대상 상품 구성과 실제 판매 상위 상품이 맞는지 함께 점검해 주세요.")

        return {
            "title": "결제/할인 민감도",
            "summary": summary,
            "metrics": metric_items[:5],
            "actions": actions[:3],
            "status": "active" if discount_context else "normal",
        }

    def _fetch_menu_mix_insight(self, store_id: str | None, date_from: str | None, date_to: str | None) -> dict | None:
        where_clause, params = self._build_filters("masked_stor_cd", "sale_dt", store_id, date_from, date_to)
        with self.engine.connect() as connection:
            top_rows = connection.execute(
                text(
                    f"""
                    SELECT
                        item_nm,
                        SUM(sale_qty) AS sale_qty,
                        SUM(net_sale_amt) AS net_sale_amt
                    FROM core_daily_item_sales
                    {where_clause}
                    GROUP BY item_nm
                    ORDER BY SUM(net_sale_amt) DESC, SUM(sale_qty) DESC
                    LIMIT 3
                    """
                ),
                params,
            ).mappings().all()
            low_rows = connection.execute(
                text(
                    f"""
                    SELECT
                        item_nm,
                        SUM(net_sale_amt) AS net_sale_amt
                    FROM core_daily_item_sales
                    {where_clause}
                    GROUP BY item_nm
                    HAVING SUM(net_sale_amt) > 0
                    ORDER BY SUM(net_sale_amt) ASC, item_nm
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().all()

        if not top_rows:
            return None

        metric_items = [
            {
                "label": "대표 상품",
                "value": str(top_rows[0]["item_nm"] or "-"),
                "detail": f"순매출 {int(float(top_rows[0]['net_sale_amt'] or 0)):,}원 / 판매 {int(float(top_rows[0]['sale_qty'] or 0)):,}개",
            }
        ]
        if len(top_rows) > 1:
            metric_items.append(
                {
                    "label": "보완 후보",
                    "value": str(top_rows[1]["item_nm"] or "-"),
                    "detail": f"순매출 {int(float(top_rows[1]['net_sale_amt'] or 0)):,}원",
                }
            )
        if low_rows:
            metric_items.append(
                {
                    "label": "저성과 점검",
                    "value": str(low_rows[0]["item_nm"] or "-"),
                    "detail": f"순매출 {int(float(low_rows[0]['net_sale_amt'] or 0)):,}원",
                }
            )

        return {
            "title": "메뉴 믹스 추천",
            "summary": f"{top_rows[0]['item_nm']} 중심으로 매출이 형성되고 있어 동반 제안 상품 운영 여지가 있습니다.",
            "metrics": metric_items,
            "actions": [
                "대표 상품과 음료 또는 세트 상품을 함께 제안해 주세요.",
                "저성과 상품은 피크타임보다 비피크 시간대 테스트로 노출해 주세요.",
            ],
            "status": "active",
        }

    def _query_yoy_comparison(self) -> dict | None:
        """전년 동월 대비 이번 달 매출 비교 (C-05)"""
        relation, amt_col = None, None
        if has_table(self.engine, "core_daily_item_sales"):
            relation, amt_col = "core_daily_item_sales", "net_sale_amt"
        elif has_table(self.engine, "raw_daily_store_item"):
            relation, amt_col = "raw_daily_store_item", "sale_amt"
        if not relation:
            return None
        try:
            with self.engine.connect() as connection:
                rows = connection.execute(
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
                ).mappings().all()
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
            }
        return {
            "text": f"{latest_yr}년 {int(latest_mo)}월 매출 데이터를 확인했습니다. 전년 동월 비교 데이터가 부족합니다.",
            "evidence": [
                f"{latest_yr}년 {int(latest_mo)}월 누적 매출: {int(current_amt):,}원",
                "전년 동월 데이터 없음",
            ],
            "actions": ["인사이트 탭에서 최신 기간별 매출 추이를 확인해 주세요."],
        }

    def _query_item_ranking(self) -> dict | None:
        """상품별 매출 순위 조회 (C-08)"""
        relation, amt_col = None, None
        if has_table(self.engine, "core_daily_item_sales"):
            relation, amt_col = "core_daily_item_sales", "net_sale_amt"
        elif has_table(self.engine, "raw_daily_store_item"):
            relation, amt_col = "raw_daily_store_item", "sale_amt"
        if not relation:
            return None
        try:
            with self.engine.connect() as connection:
                rows = connection.execute(
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
                ).mappings().all()
        except SQLAlchemyError:
            return None

        if not rows:
            return None

        top_item = str(rows[0]["item_nm"])
        evidence = [
            f"{str(row['item_nm'])}: {int(float(row['total_amt'] or 0)):,}원 / {int(float(row['total_qty'] or 0)):,}개"
            for row in rows[:4]
        ]
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
            rows = connection.execute(
                text(
                    f"""
                    SELECT
                        COALESCE(NULLIF(ho_chnl_div, ''), '기타') AS channel_div,
                        SUM(sale_amt) AS sale_amt,
                        SUM(ord_cnt) AS ord_cnt
                    FROM {source_relation}
                    GROUP BY channel_div
                    ORDER BY sale_amt DESC
                    LIMIT 5
                    """
                )
            ).mappings().all()
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
                            if total_amt > 0 else "0%"
                        ),
                        "peer_value": "-",
                    }
                    for row in rows[:3]
                ],
            },
        }
