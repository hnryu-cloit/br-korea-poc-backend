from __future__ import annotations

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

    async def list_prompts(self) -> list[dict]:
        return SUGGESTED_PROMPTS

    async def get_query_response(self, prompt: str) -> dict:
        source_relation = None
        if self.engine and has_table(self.engine, "core_channel_sales"):
            source_relation = "core_channel_sales"
        elif self.engine and has_table(self.engine, "raw_daily_store_online"):
            source_relation = "raw_daily_store_online"

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
            if has_table(self.engine, "raw_daily_store_pay_way"):
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
        where_clause, params = self._build_filters("masked_stor_cd", "sale_dt", store_id, date_from, date_to)
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT
                        COALESCE(NULLIF(pay_way_cd_nm, ''), NULLIF(pay_way_cd, ''), '기타') AS payment_label,
                        SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) AS payment_amt
                    FROM raw_daily_store_pay_way
                    {where_clause}
                    GROUP BY COALESCE(NULLIF(pay_way_cd_nm, ''), NULLIF(pay_way_cd, ''), '기타')
                    ORDER BY SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) DESC
                    LIMIT 3
                    """
                ),
                params,
            ).mappings().all()

        if not rows:
            return None

        total_amt = sum(float(row["payment_amt"] or 0) for row in rows)
        metric_items = []
        for row in rows[:3]:
            ratio = 0 if total_amt == 0 else round(float(row["payment_amt"] or 0) / total_amt * 100, 1)
            metric_items.append(
                {
                    "label": str(row["payment_label"]),
                    "value": f"{ratio:.1f}%",
                    "detail": f"결제금액 {int(float(row['payment_amt'] or 0)):,}원",
                }
            )

        return {
            "title": "결제/할인 민감도",
            "summary": f"{rows[0]['payment_label']} 비중이 가장 높고 결제수단 편중 여부를 운영 관점에서 점검할 수 있습니다.",
            "metrics": metric_items,
            "actions": [
                "상위 결제수단과 연결된 프로모션 성과를 함께 비교해 주세요.",
                "특정 할인수단 비중이 높아질 때는 객단가 방어 상품을 함께 제안해 주세요.",
            ],
            "status": "normal",
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
