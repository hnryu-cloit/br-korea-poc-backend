from typing import Any, Optional

from pydantic import BaseModel, Field


class AuditLogEntry(BaseModel):
    id: int
    timestamp: str
    domain: str
    event_type: str
    actor_role: str
    route: str
    outcome: str
    message: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class AuditLogListResponse(BaseModel):
    items: list[AuditLogEntry]
    total: int
    filtered_domain: Optional[str] = None
