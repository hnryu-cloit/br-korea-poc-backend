from typing import Any

from pydantic import BaseModel

from app.schemas.explainability import ExplainabilityPayload


class NotificationItem(BaseModel):
    id: int
    category: str
    title: str
    description: str
    created_at: str
    unread: bool
    link_to: str | None = None
    link_state: dict[str, Any] | None = None
    action_label: str = "확인하기"
    evidence_hint: str | None = None
    explainability: ExplainabilityPayload | None = None


class NotificationListResponse(BaseModel):
    items: list[NotificationItem]
    unread_count: int
