from __future__ import annotations

from datetime import datetime
from typing import Any

from app.repositories.audit_repository import AuditRepository


def _relative_time(timestamp_str: str) -> str:
    """audit_logs timestamp 문자열을 '방금 전', 'N분 전' 형식으로 변환합니다."""
    try:
        ts = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        diff = int((datetime.now() - ts).total_seconds() / 60)
        if diff < 1:
            return "방금 전"
        if diff < 60:
            return f"{diff}분 전"
        hours = diff // 60
        if hours < 24:
            return f"{hours}시간 전"
        return f"{diff // 1440}일 전"
    except (ValueError, TypeError):
        return ""


class NotificationsRepository:
    def __init__(self, audit_repository: AuditRepository) -> None:
        self.audit_repository = audit_repository

    async def get_recent_sales_notification(
        self, store_id: str | None = None
    ) -> dict[str, Any] | None:
        entries = await self.audit_repository.list_entries(
            domain="sales", limit=1, store_id=store_id
        )
        if not entries:
            return None
        entry = entries[0]
        entry_id = entry.get("id", 3)
        timestamp = entry.get("timestamp", "")
        metadata = entry.get("metadata") or {}
        prompt = metadata.get("prompt") if isinstance(metadata, dict) else None
        description = "최근 매출 질의 응답과 액션 아이템을 확인할 수 있습니다."
        if isinstance(prompt, str) and prompt:
            description = prompt
        return {
            "id": entry_id,
            "category": "analysis",
            "title": "매출 질의 응답 준비",
            "description": description,
            "created_at": _relative_time(timestamp),
            "unread": False,
            "link_to": "/sales",
            "link_state": {
                "source": "notification",
                "notificationId": entry_id,
                "prompt": description,
            },
        }
