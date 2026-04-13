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
        sales_status = self._build_sales_status(ordering_summary=ordering_summary, production_danger_count=production.danger_count)

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
            sales_status=sales_status,
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
                        f"{danger_item.name} 현재 재고 {danger_item.current}개와 예측 재고 {danger_item.forecast}개 차이로 "
                        f"{danger_item.depletion_time} 전 소진 위험이 계산되었습니다."
                    ),
                    confidence_score=self._calculate_confidence_score(danger_item.current, danger_item.forecast),
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
                ai_reasoning=(
                    f"최근 선택 {ordering_summary.total}건, 최근 7일 선택 {ordering_summary.recent_selection_count_7d}건을 기준으로 "
                    "마감 전 검토 필요도를 계산했습니다."
                ),
                confidence_score=self._calculate_ordering_confidence(ordering_summary.total),
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
                ai_reasoning=(
                    "매장 생산 대상이 아닌 품목 상태로 분류되어 생산 CTA를 비활성화했습니다."
                ),
                confidence_score=0.99,
                is_finished_good=True,
            )
        )

        return actions[:3]

    @staticmethod
    def _build_stats(danger_count: int, ordering_deadline_minutes: int, alert_count: int) -> list[HomeStatItem]:
        return [
            HomeStatItem(key="production_risk_count", label="품절 위험 SKU", value=f"{danger_count}개", tone="danger"),
            HomeStatItem(key="ordering_deadline_minutes", label="주문 마감까지", value=f"{ordering_deadline_minutes}분", tone="primary"),
            HomeStatItem(key="today_profit_estimate", label="오늘 운영 상태", value=("위험 있음" if danger_count else "안정"), tone="success" if danger_count == 0 else "default"),
            HomeStatItem(key="alert_count", label="알림 상태", value=f"긴급 {alert_count}건", tone="default"),
        ]

    def _build_cards(
        self,
        production,
        ordering_summary,
        ordering_deadline_minutes: int,
        ordering_option_count: int,
        sales_status: dict[str, str],
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
                HomeCardMetric(label="추천 기준", value=f"옵션 {ordering_option_count}개", tone="default"),
            ],
            cta_label="주문 검토하기",
            cta_path="/ordering",
            prompts=["추천 주문량은?", "최근 선택 현황은?", "마감 전 확인 항목은?"],
            status_label="검토 필요" if not ordering_summary.recommended_selected else "선택 완료",
            deadline_minutes=ordering_deadline_minutes,
            delivery_scheduled=ordering_summary.total > 0,
        )

        sales_card = HomeSummaryCard(
            domain="sales",
            title="손익 분석",
            description="현재 운영 데이터 기반 상태 요약",
            highlights=[
                sales_status["headline"],
                sales_status["detail"],
            ],
            metrics=[
                HomeCardMetric(label="위험 SKU", value=f"{production.danger_count}개", tone="danger" if production.danger_count else "success"),
                HomeCardMetric(label="주문 선택", value=f"{ordering_summary.total}건", tone="default"),
            ],
            cta_label="손익분석 상세보기",
            cta_path="/sales",
            prompts=["최근 매출 인사이트는?", "위험 요인은?", "운영 상태는?"],
            status_label=sales_status["status_label"],
        )

        return [production_card, ordering_card, sales_card]

    @staticmethod
    def _calculate_confidence_score(current: int, forecast: int) -> float:
        denominator = max(current + forecast, 1)
        return round(min(0.99, 0.55 + (abs(current - forecast) / denominator) * 0.4), 2)

    @staticmethod
    def _calculate_ordering_confidence(total_orders: int) -> float:
        return round(min(0.99, 0.5 + min(total_orders, 10) * 0.04), 2)

    @staticmethod
    def _build_sales_status(ordering_summary, production_danger_count: int) -> dict[str, str]:
        if production_danger_count > 0:
            return {
                "headline": f"생산 위험 SKU {production_danger_count}개가 운영 상태에 직접 영향을 주고 있습니다.",
                "detail": f"주문 선택 {ordering_summary.total}건, 최근 7일 선택 {ordering_summary.recent_selection_count_7d}건 기준으로 추가 확인이 필요합니다.",
                "status_label": "주의",
            }
        if ordering_summary.total > 0:
            return {
                "headline": "주문 선택 이력이 존재하며 생산 위험 SKU는 없습니다.",
                "detail": f"최근 선택 {ordering_summary.total}건이 기록되어 현재 운영 상태는 안정 범위입니다.",
                "status_label": "안정",
            }
        return {
            "headline": "운영 데이터가 제한적입니다.",
            "detail": "주문 선택 또는 생산 위험 데이터가 충분하지 않아 상세 손익 판단은 제한됩니다.",
            "status_label": "데이터 확인",
        }
