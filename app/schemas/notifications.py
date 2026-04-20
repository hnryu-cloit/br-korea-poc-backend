from typing import Any

from pydantic import BaseModel


class NotificationItem(BaseModel):
    id: int
    category: str
    title: str
    description: str
    created_at: str
    unread: bool
    link_to: str | None = None
    link_state: dict[str, Any] | None = None


class NotificationListResponse(BaseModel):
    items: list[NotificationItem]
    unread_count: int
