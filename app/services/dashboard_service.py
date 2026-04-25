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

    async def get_alerts(
        self,
        payload: DashboardHomeRequest,
        reference_datetime: datetime | None = None,
    ) -> DashboardAlertsResponse:
        production = await self.production_service.get_overview(
            store_id=payload.store_id,
            business_date=payload.business_date,
            reference_datetime=reference_datetime,
        )
        ordering_deadline = await self.ordering_service.get_deadline(
            store_id=payload.store_id,
            reference_datetime=reference_datetime,
            allow_ai=False,
        )

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

        deadline_at = self._deadline_to_iso(
            ordering_deadline.get("deadline"),
            base_datetime=reference_datetime,
        )
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
        reference_datetime: datetime | None = None,
    ) -> DashboardSummaryCardsResponse:
        production = await self.production_service.get_overview(
            store_id=payload.store_id,
            business_date=payload.business_date,
            reference_datetime=reference_datetime,
        )
        production_rows = await self.production_service.repository.list_items(
            store_id=payload.store_id,
            business_date=payload.business_date,
            reference_datetime=reference_datetime,
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
            reference_datetime=reference_datetime,
            allow_ai_deadline=False,
        )
        sales = await self.sales_service.get_dashboard_overview(
            store_id=payload.store_id,
            business_date=payload.business_date,
            reference_datetime=reference_datetime,
        )
        recommended_questions = await self.repository.get_dashboard_recommended_questions(
            target_date=self._resolve_date(payload.business_date),
        )

        production_items = sorted(
            production.items,
            key=lambda item: (
                0 if item.status == "danger" else 1 if item.status == "warning" else 2,
                int(item.current),
            ),
        )[:5]
        hourly_sale_by_sku = {
            str(row.get("sku_id") or ""): max(int(row.get("hourly_sale_qty") or 0), 0)
            for row in production_rows
        }

        deadline_label = self._build_deadline_label(ordering_options.deadline_at)
        top_option = next(
            (option for option in ordering_options.options if option.recommended),
            ordering_options.options[0] if ordering_options.options else None,
        )

        ordering_items: list[DashboardOrderingDeadlineItem] = []
        for deadline_item in ordering_options.deadline_items[:3]:
            ordering_items.append(
                DashboardOrderingDeadlineItem(
                    name=deadline_item.sku_name,
                    deadline_time=self._build_deadline_label(deadline_item.deadline_at)
                    or deadline_label,
                )
            )

        cards = [
            DashboardProductionSummaryCard(
                title="생산 현황",
                cta_path="/production",
                recommended_questions=recommended_questions.get("production", []),
                top_products=[
                    DashboardProductionSummaryItem(
                        name=item.name,
                        current_stock=int(item.current),
                        predicted_consumption_1h=hourly_sale_by_sku.get(item.sku_id, 0),
                    )
                    for item in production_items
                ],
            ),
            DashboardOrderingSummaryCard(
                title="주문 관리",
                cta_path="/ordering",
                recommended_questions=recommended_questions.get("ordering", []),
                ai_order_basis=top_option.basis if top_option else "최근 주문 이력",
                ai_order_cta_path="/ordering",
                deadline_products=ordering_items,
            ),
            DashboardSalesSummaryCard(
                title="손익분석",
                cta_path="/sales",
                recommended_questions=recommended_questions.get("sales", []),
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
    def _deadline_to_iso(
        deadline: str | None,
        base_datetime: datetime | None = None,
    ) -> str | None:
        if not deadline:
            return None
        try:
            hour_str, minute_str = deadline.split(":")
            base = base_datetime or get_now()
            target = base.replace(
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
