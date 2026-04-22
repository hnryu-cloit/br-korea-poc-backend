from __future__ import annotations

import asyncio

from app.repositories.notifications_repository import NotificationsRepository
from app.schemas.notifications import NotificationItem, NotificationListResponse
from app.services.explainability_service import create_ready_payload
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
                    action_label="생산 화면에서 권장 수량 확인",
                    evidence_hint=f"예상 재고 {alert.forecast}개 / 권장 생산 {alert.recommended}개",
                    explainability=create_ready_payload(
                        trace_id=f"noti-production-{alert.sku_id}",
                        actions=["생산관리 화면으로 이동해 권장 수량을 반영하세요."],
                        evidence=[f"현재 재고 {alert.current} / 1시간 예측 {alert.forecast}"],
                    ),
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
                    action_label="주문 추천안 검토 후 최종 선택",
                    evidence_hint=f"주문 마감까지 {alert.deadline_minutes}분",
                    explainability=create_ready_payload(
                        trace_id=f"noti-ordering-{alert.notification_id}",
                        actions=["주문관리 화면에서 추천안 3개를 비교하고 최종안 확정하세요."],
                        evidence=[f"주문 마감까지 {alert.deadline_minutes}분"],
                    ),
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
                            action_label="위험 SKU 생산 여부 즉시 확인",
                            evidence_hint=f"severity={severity}",
                            explainability=create_ready_payload(
                                trace_id=f"noti-ai-{alert.get('sku_id') or idx}",
                                actions=["생산 경고 SKU를 우선 점검하고 필요 시 즉시 생산하세요."],
                                evidence=[f"AI 알림 심각도: {severity}"],
                            ),
                        )
                    )
            except Exception:
                pass  # AI 서비스 연결 실패 시 무시

        if not isinstance(sales_item, Exception) and sales_item is not None:
            payload = dict(sales_item)
            payload.setdefault("action_label", "매출 분석 화면에서 원인 확인")
            payload.setdefault("evidence_hint", "최근 매출 이상 탐지 기반 알림")
            payload.setdefault(
                "explainability",
                create_ready_payload(
                    trace_id="noti-sales-latest",
                    actions=["매출 분석 화면으로 이동해 원인과 대응 액션을 확인하세요."],
                    evidence=["최근 매출 추세 이상 탐지 이벤트 기반"],
                ),
            )
            items.append(NotificationItem(**payload))

        unread_count = sum(1 for item in items if item.unread)
        return NotificationListResponse(items=items, unread_count=unread_count)
