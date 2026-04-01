from __future__ import annotations

from typing import Any

from app.repositories.audit_repository import AuditRepository


class NotificationsRepository:
    def __init__(self, audit_repository: AuditRepository) -> None:
        self.audit_repository = audit_repository

    async def get_recent_sales_notification(self) -> dict[str, Any] | None:
        entries = await self.audit_repository.list_entries(domain="sales", limit=1)
        if not entries:
            return None
        entry = entries[0]
        metadata = entry.get("metadata") or {}
        prompt = metadata.get("prompt") if isinstance(metadata, dict) else None
        description = "최근 매출 질의 응답과 액션 아이템을 확인할 수 있습니다."
        if isinstance(prompt, str) and prompt:
            description = prompt
        return {
            "id": 3,
            "category": "analysis",
            "title": "매출 질의 응답 준비",
            "description": description,
            "created_at": "18분 전",
            "unread": False,
            "link_to": "/sales",
            "link_state": {"source": "notification", "notificationId": 3, "prompt": description},
        }
