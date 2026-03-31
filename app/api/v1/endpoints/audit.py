from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.auth import require_roles
from app.core.deps import get_audit_service
from app.schemas.audit import AuditLogListResponse
from app.services.audit_service import AuditService

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/logs", response_model=AuditLogListResponse)
async def list_audit_logs(
    domain: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    _: str = Depends(require_roles("hq_operator", "hq_planner")),
    service: AuditService = Depends(get_audit_service),
) -> AuditLogListResponse:
    return await service.list_logs(domain=domain, limit=limit)
