from __future__ import annotations

from datetime import datetime
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
        if not entry:
            # 저장소가 비활성/실패여도 서비스 플로우를 중단하지 않도록 안전 응답을 구성한다.
            entry = {
                "id": 0,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "domain": domain,
                "event_type": event_type,
                "actor_role": actor_role,
                "route": route,
                "outcome": outcome,
                "message": message,
                "metadata": metadata or {},
            }
        return AuditLogEntry(**entry)

    async def list_logs(self, domain: str | None = None, limit: int = 50) -> AuditLogListResponse:
        entries = await self.repository.list_entries(domain=domain, limit=limit)
        return AuditLogListResponse(
            items=[AuditLogEntry(**entry) for entry in entries],
            total=len(entries),
            filtered_domain=domain,
        )
