from __future__ import annotations

from datetime import datetime

from app.core.utils import get_now
from app.repositories.home_repository import HomeRepository
from app.schemas.dashboard import (
    DashboardAlertsResponse,
    DashboardHomeRequest,
    DashboardLowStockProduct,
    DashboardNoticeItem,
    DashboardNoticesResponse,
    DashboardOrderDeadline,
    DashboardOrderingDeadlineItem,
    DashboardOrderingSummaryCard,
    DashboardProductionSummaryCard,
    DashboardProductionSummaryItem,
    DashboardSalesOverview,
    DashboardSalesSummaryCard,
    DashboardSummaryCardsResponse,
)
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService
from app.services.sales_service import SalesService


class DashboardService:
    def __init__(
        self,
        production_service: ProductionService,
        ordering_service: OrderingService,
        sales_service: SalesService,
        repository: HomeRepository,
    ) -> None:
        self.production_service = production_service
        self.ordering_service = ordering_service
        self.sales_service = sales_service
        self.repository = repository

    async def get_notices(self, payload: DashboardHomeRequest) -> DashboardNoticesResponse:
        target_date = self._resolve_date(payload.business_date)
        events = await self.repository.list_schedule_events(
            store_id=payload.store_id,
            today=target_date,
        )
        items = [self._build_notice_item(event) for event in events[:10]]
        return DashboardNoticesResponse(items=items)

    async def get_alerts(self, payload: DashboardHomeRequest) -> DashboardAlertsResponse:
        production = await self.production_service.get_overview(
            store_id=payload.store_id,
            business_date=payload.business_date,
        )
        ordering_deadline = await self.ordering_service.get_deadline(store_id=payload.store_id)

        low_stock_products = [
            DashboardLowStockProduct(
                id=item.sku_id,
                name=item.name,
                remaining_stock=max(int(item.current), 0),
                cta_path="/production",
            )
            for item in production.items
            if item.status in {"danger", "warning"}
        ][:12]

        deadline_at = self._deadline_to_iso(ordering_deadline.get("deadline"))
        order_deadline = (
            DashboardOrderDeadline(deadline_at=deadline_at, cta_path="/ordering")
            if deadline_at
            else None
        )

        return DashboardAlertsResponse(
            low_stock_products=low_stock_products,
            order_deadline=order_deadline,
        )

    async def get_summary_cards(
        self,
        payload: DashboardHomeRequest,
    ) -> DashboardSummaryCardsResponse:
        production = await self.production_service.get_overview(
            store_id=payload.store_id,
            business_date=payload.business_date,
        )
        ordering_summary = await self.ordering_service.get_selection_summary(
            store_id=payload.store_id,
            date_from=payload.business_date,
            date_to=payload.business_date,
        )
        ordering_options = await self.ordering_service.list_options(
            notification_entry=False,
            store_id=payload.store_id,
            skip_ai=True,
        )
        sales = await self.sales_service.get_dashboard_overview(
            store_id=payload.store_id,
            business_date=payload.business_date,
        )

        production_items = sorted(
            production.items,
            key=lambda item: (
                0 if item.status == "danger" else 1 if item.status == "warning" else 2,
                int(item.current),
            ),
        )[:5]

        deadline_label = self._build_deadline_label(ordering_options.deadline_at)
        top_option = next(
            (option for option in ordering_options.options if option.recommended),
            ordering_options.options[0] if ordering_options.options else None,
        )

        ordering_items: list[DashboardOrderingDeadlineItem] = []
        for option in ordering_options.options[:3]:
            first_item = option.items[0] if option.items else None
            ordering_items.append(
                DashboardOrderingDeadlineItem(
                    name=first_item.sku_name if first_item else option.title,
                    deadline_time=deadline_label,
                )
            )

        cards = [
            DashboardProductionSummaryCard(
                title="생산 현황",
                cta_path="/production",
                recommended_questions=[
                    "오늘 오후 피크타임 전에 어떤 상품을 더 만들어야 할까?",
                    "대표 메뉴 중 지금 가장 빨리 소진될 상품은 뭐야?",
                    "현재 재고 기준으로 1시간 뒤 위험한 상품만 골라줘",
                ],
                top_products=[
                    DashboardProductionSummaryItem(
                        name=item.name,
                        current_stock=int(item.current),
                        predicted_consumption_1h=max(int(item.forecast) - int(item.current), 0),
                    )
                    for item in production_items
                ],
            ),
            DashboardOrderingSummaryCard(
                title="주문 관리",
                cta_path="/ordering",
                recommended_questions=[
                    "오늘 발주 추천안은 어떤 기준으로 만든 거야?",
                    "지금 바로 확인해야 할 발주 마감 상품만 알려줘",
                    "지난주 같은 요일과 비교해서 더 주문해야 할 품목은 뭐야?",
                ],
                ai_order_basis=top_option.basis if top_option else "최근 주문 이력",
                ai_order_cta_path="/ordering",
                deadline_products=ordering_items,
            ),
            DashboardSalesSummaryCard(
                title="손익분석",
                cta_path="/sales",
                recommended_questions=[
                    "오늘 매출은 지난달 같은 요일 평균보다 얼마나 높아?",
                    "현재 시간대 매출 흐름이 좋은 편인지 알려줘",
                    "이번달 누적 매출 기준으로 목표 달성 가능성을 보여줘",
                ],
                sales_overview=DashboardSalesOverview(**sales),
            ),
        ]

        return DashboardSummaryCardsResponse(
            updated_at=get_now().isoformat(timespec="seconds"),
            cards=cards,
        )

    @staticmethod
    def _resolve_date(value: str | None):
        if not value:
            return get_now().date()
        return datetime.strptime(value, "%Y-%m-%d").date()

    @staticmethod
    def _build_notice_item(event: dict[str, str]) -> DashboardNoticeItem:
        category = str(event.get("category") or "notice")
        title = str(event.get("title") or "운영 안내")
        if category == "campaign":
            return DashboardNoticeItem(
                id=f"{category}-{event.get('date')}-{title}",
                name=title,
                tag="프로모션",
            )
        if category == "telecom":
            return DashboardNoticeItem(
                id=f"{category}-{event.get('date')}-{title}",
                name=title,
                tag="제휴",
            )
        return DashboardNoticeItem(
            id=f"{category}-{event.get('date')}-{title}",
            name=title,
            tag="공지",
        )

    @staticmethod
    def _deadline_to_iso(deadline: str | None) -> str | None:
        if not deadline:
            return None
        try:
            hour_str, minute_str = deadline.split(":")
            now = get_now()
            target = now.replace(
                hour=int(hour_str),
                minute=int(minute_str),
                second=0,
                microsecond=0,
            )
        except (TypeError, ValueError):
            return None
        return target.isoformat(timespec="seconds")

    @staticmethod
    def _build_deadline_label(deadline_at: str | None) -> str:
        if not deadline_at:
            return "-"
        if "T" in deadline_at:
            return deadline_at.split("T", 1)[1][:5]
        return deadline_at[:5]
