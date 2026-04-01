from __future__ import annotations

from app.repositories.notifications_repository import NotificationsRepository
from app.schemas.notifications import NotificationItem, NotificationListResponse
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService


class NotificationsService:
    def __init__(
        self,
        ordering_service: OrderingService,
        production_service: ProductionService,
        repository: NotificationsRepository,
    ) -> None:
        self.ordering_service = ordering_service
        self.production_service = production_service
        self.repository = repository

    async def list_notifications(self) -> NotificationListResponse:
        items: list[NotificationItem] = []

        production_alerts = await self.production_service.get_alerts()
        if production_alerts.alerts:
            alert = production_alerts.alerts[0]
            items.append(
                NotificationItem(
                    id=1,
                    category="alert",
                    title="생산 알림 발송 필요",
                    description=alert.push_message,
                    created_at="방금 전",
                    unread=True,
                    link_to="/production",
                    link_state=None,
                )
            )

        ordering_alerts = await self.ordering_service.list_deadline_alerts(before_minutes=20)
        if ordering_alerts.alerts:
            alert = ordering_alerts.alerts[0]
            items.append(
                NotificationItem(
                    id=2,
                    category="workflow",
                    title="주문 추천 생성 완료",
                    description=alert.message,
                    created_at="4분 전",
                    unread=True,
                    link_to=alert.target_path,
                    link_state={"source": "notification", "notificationId": alert.notification_id, "focusOptionId": alert.focus_option_id},
                )
            )

        sales_item = await self.repository.get_recent_sales_notification()
        if sales_item is not None:
            items.append(NotificationItem(**sales_item))

        unread_count = sum(1 for item in items if item.unread)
        return NotificationListResponse(items=items, unread_count=unread_count)
