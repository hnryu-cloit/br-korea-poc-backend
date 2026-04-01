from typing import Any, Optional

from pydantic import BaseModel


class NotificationItem(BaseModel):
    id: int
    category: str
    title: str
    description: str
    created_at: str
    unread: bool
    link_to: Optional[str] = None
    link_state: Optional[dict[str, Any]] = None


class NotificationListResponse(BaseModel):
    items: list[NotificationItem]
    unread_count: int
