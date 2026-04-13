from __future__ import annotations

from datetime import datetime

from app.schemas.home import (
    HomeCardMetric,
    HomeOverviewRequest,
    HomeOverviewResponse,
    HomePriorityAction,
    HomeStatItem,
    HomeSummaryCard,
)
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService


class HomeService:
    def __init__(self, production_service: ProductionService, ordering_service: OrderingService) -> None:
        self.production_service = production_service
        self.ordering_service = ordering_service

    async def get_overview(self, payload: HomeOverviewRequest) -> HomeOverviewResponse:
        production = await self.production_service.get_overview()
        ordering_summary = await self.ordering_service.get_selection_summary(
            store_id=payload.store_id,
            date_from=payload.business_date,
            date_to=payload.business_date,
        )
        ordering_options = await self.ordering_service.list_options(notification_entry=False)

        priority_actions = self._build_priority_actions(production=production, ordering_summary=ordering_summary)
        stats = self._build_stats(
            danger_count=production.danger_count,
            ordering_deadline_minutes=ordering_options.deadline_minutes,
            alert_count=production.danger_count + (1 if ordering_summary.summary_status != "recommended_selected" else 0),
        )
        cards = self._build_cards(
            production=production,
            ordering_summary=ordering_summary,
            ordering_deadline_minutes=ordering_options.deadline_minutes,
            ordering_option_count=len(ordering_options.options),
        )

        return HomeOverviewResponse(
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            priority_actions=priority_actions,
            stats=stats,
            cards=cards,
        )

    def _build_priority_actions(self, production, ordering_summary) -> list[HomePriorityAction]:
        danger_item = next((item for item in production.items if item.status == "danger"), production.items[0] if production.items else None)
        warning_item = next((item for item in production.items if item.status == "warning"), None)

        actions: list[HomePriorityAction] = []
        if danger_item is not None:
            actions.append(
                HomePriorityAction(
                    id=f"production-{danger_item.sku_id}",
                    type="production",
                    urgency="urgent",
                    badge_label=f"긴급 · 재고 소진 {danger_item.depletion_time}",
                    title=f"{danger_item.name} 생산 필요",
                    description=(
                        f"현재 {danger_item.current}개, 1시간 후 {danger_item.forecast}개 예상 · "
                        f"권장 생산 {danger_item.recommended}개"
                    ),
                    cta_label="생산하기",
                    cta_path="/production",
                    focus_section=danger_item.sku_id,
                    related_sku_id=danger_item.sku_id,
                    ai_reasoning=(
                        f"{danger_item.name} 판매 속도가 평소보다 빨라 {danger_item.depletion_time} 전에 소진될 가능성이 높습니다."
                    ),
                    confidence_score=0.92,
                    is_finished_good=False,
                )
            )

        actions.append(
            HomePriorityAction(
                id="ordering-deadline",
                type="ordering",
                urgency="important",
                badge_label="중요 · 주문 마감 임박",
                title="주문 마감 확인 필요",
                description=(
                    f"최근 주문 선택 {ordering_summary.total}건 · "
                    f"현재 상태: {'추천안 선택 완료' if ordering_summary.recommended_selected else '검토 필요'}"
                ),
                cta_label="주문 검토하기",
                cta_path="/ordering",
                focus_section="summary",
                ai_reasoning="주문 마감 전 추천안 검토 이력이 충분하지 않아 누락 방지 확인이 필요합니다.",
                confidence_score=0.88,
                is_finished_good=False,
            )
        )

        actions.append(
            HomePriorityAction(
                id=f"production-finished-{warning_item.sku_id if warning_item else 'hq'}",
                type="production",
                urgency="recommended",
                badge_label="권장 · 본사 납품 완제품",
                title=(f"{warning_item.name} 추가 생산 제외" if warning_item else "완제품 납품 품목은 생산 제외"),
                description=(
                    "본사 납품 완제품은 매장 생산 대상이 아니므로 생산 버튼을 비활성화합니다."
                ),
                cta_label="생산관리 보기",
                cta_path="/production",
                focus_section=warning_item.sku_id if warning_item else None,
                related_sku_id=warning_item.sku_id if warning_item else None,
                ai_reasoning="해당 품목은 본사 납품 완제품으로 분류되어 매장 생산 리드타임 계산 대상에서 제외됩니다.",
                confidence_score=0.97,
                is_finished_good=True,
            )
        )

        return actions[:3]

    @staticmethod
    def _build_stats(danger_count: int, ordering_deadline_minutes: int, alert_count: int) -> list[HomeStatItem]:
        return [
            HomeStatItem(key="production_risk_count", label="품절 위험 SKU", value=f"{danger_count}개", tone="danger"),
            HomeStatItem(key="ordering_deadline_minutes", label="주문 마감까지", value=f"{ordering_deadline_minutes}분", tone="primary"),
            HomeStatItem(key="today_profit_estimate", label="오늘 순이익 추정", value="+342,000원", tone="success"),
            HomeStatItem(key="alert_count", label="알림 상태", value=f"긴급 {alert_count}건", tone="default"),
        ]

    def _build_cards(
        self,
        production,
        ordering_summary,
        ordering_deadline_minutes: int,
        ordering_option_count: int,
    ) -> list[HomeSummaryCard]:
        danger_items = [item for item in production.items if item.status in {"danger", "warning"}][:2]
        production_highlights = [
            f"{item.name} · 현재 {item.current}개 / 1시간 후 {item.forecast}개 예상"
            for item in danger_items
        ] or ["현재 위험 품목이 없습니다."]

        production_card = HomeSummaryCard(
            domain="production",
            title="생산 현황",
            description="실시간 재고 및 1시간 후 예측",
            highlights=production_highlights,
            metrics=[
                HomeCardMetric(label="품절 위험", value=f"{production.danger_count}개", tone="danger"),
                HomeCardMetric(label="생산 리드타임", value=f"{production.production_lead_time_minutes}분", tone="primary"),
            ],
            cta_label="생산관리 상세보기",
            cta_path="/production",
            prompts=["지금 생산해야 할 품목은?", "찬스 로스가 뭔가요?", "품절 처리 방법은?"],
            status_label="즉시 확인",
        )

        ordering_card = HomeSummaryCard(
            domain="ordering",
            title="주문 관리",
            description="주문 누락 방지 및 추천 검토",
            highlights=[
                f"주문 상태 · {'추천안 선택 완료' if ordering_summary.recommended_selected else '검토 필요'}",
                f"AI 추천안 {ordering_option_count}개 준비됨 · 최근 7일 선택 {ordering_summary.recent_selection_count_7d}건",
            ],
            metrics=[
                HomeCardMetric(label="주문 마감", value=f"{ordering_deadline_minutes}분 남음", tone="primary"),
                HomeCardMetric(label="추천 기준", value="전일 / 전주 / 패턴", tone="default"),
            ],
            cta_label="주문 검토하기",
            cta_path="/ordering",
            prompts=["추천 주문량은?", "어제와 비교하면?", "날씨 영향은?"],
            status_label="검토 필요" if not ordering_summary.recommended_selected else "선택 완료",
            deadline_minutes=ordering_deadline_minutes,
            delivery_scheduled=True,
        )

        sales_card = HomeSummaryCard(
            domain="sales",
            title="손익 분석",
            description="순이익 및 손익분기점 분석",
            highlights=[
                "어제 대비 매출 15% 증가",
                "손익분기점을 초과 달성 중이며 객단가가 안정적으로 유지됩니다.",
            ],
            metrics=[
                HomeCardMetric(label="오늘 순이익", value="+342,000원", tone="success"),
                HomeCardMetric(label="손익분기점", value="초과 달성", tone="success"),
            ],
            cta_label="손익분석 상세보기",
            cta_path="/sales",
            prompts=["오늘 순이익은?", "손익분기점은?", "어제와 비교하면?"],
            status_label="권장 확인",
        )

        return [production_card, ordering_card, sales_card]
