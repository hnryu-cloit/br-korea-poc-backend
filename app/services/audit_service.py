from __future__ import annotations

from typing import Any

from app.repositories.audit_repository import AuditRepository
from app.schemas.audit import AuditLogEntry, AuditLogListResponse


class AuditService:
    def __init__(self, repository: AuditRepository) -> None:
        self.repository = repository

    async def record(
        self,
        *,
        domain: str,
        event_type: str,
        actor_role: str,
        route: str,
        outcome: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> AuditLogEntry:
        entry = await self.repository.add_entry(
            domain=domain,
            event_type=event_type,
            actor_role=actor_role,
            route=route,
            outcome=outcome,
            message=message,
            metadata=metadata,
        )
        return AuditLogEntry(**entry)

    async def list_logs(self, domain: str | None = None, limit: int = 50) -> AuditLogListResponse:
        entries = await self.repository.list_entries(domain=domain, limit=limit)
        return AuditLogListResponse(
            items=[AuditLogEntry(**entry) for entry in entries],
            total=len(entries),
            filtered_domain=domain,
        )
