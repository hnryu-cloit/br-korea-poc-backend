from __future__ import annotations

import asyncio

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
        ai_client: object | None = None,
    ) -> None:
        self.ordering_service = ordering_service
        self.production_service = production_service
        self.repository = repository
        self.ai_client = ai_client

    async def list_notifications(self, store_id: str | None = None) -> NotificationListResponse:
        items: list[NotificationItem] = []

        production_result, ordering_result, sales_item = await asyncio.gather(
            self.production_service.get_alerts(store_id=store_id),
            self.ordering_service.list_deadline_alerts(before_minutes=20, store_id=store_id),
            self.repository.get_recent_sales_notification(store_id=store_id),
            return_exceptions=True,
        )

        if not isinstance(production_result, Exception) and production_result.alerts:
            alert = production_result.alerts[0]
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

        if not isinstance(ordering_result, Exception) and ordering_result.alerts:
            alert = ordering_result.alerts[0]
            items.append(
                NotificationItem(
                    id=2,
                    category="workflow",
                    title="주문 추천 생성 완료",
                    description=alert.message,
                    created_at="4분 전",
                    unread=True,
                    link_to=alert.target_path,
                    link_state={
                        "source": "notification",
                        "notificationId": alert.notification_id,
                        "focusOptionId": alert.focus_option_id,
                    },
                )
            )

        # AI 서비스 생산 PUSH 알림 병합 (ai_client 연결 시)
        if self.ai_client is not None:
            try:
                push_alerts = await self.ai_client.get_production_push_alerts(store_id=store_id)
                for idx, alert in enumerate(push_alerts[:2], start=10):
                    severity = alert.get("severity", "medium")
                    items.append(
                        NotificationItem(
                            id=idx,
                            category="alert",
                            title=alert.get("title", "생산 알림"),
                            description=alert.get("body", ""),
                            created_at="방금 전",
                            unread=True,
                            link_to="/production",
                            link_state={"sku_id": alert.get("sku_id"), "severity": severity},
                        )
                    )
            except Exception:
                pass  # AI 서비스 연결 실패 시 무시

        if not isinstance(sales_item, Exception) and sales_item is not None:
            items.append(NotificationItem(**sales_item))

        unread_count = sum(1 for item in items if item.unread)
        return NotificationListResponse(items=items, unread_count=unread_count)
