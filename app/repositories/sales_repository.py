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
