from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.core.auth import require_roles
from app.core.deps import get_audit_service
from app.schemas.audit import AuditLogListResponse
from app.services.audit_service import AuditService

router = APIRouter(prefix="/audit", tags=["audit"])

_HQ_ROLES = ("hq_admin", "hq_operator")


@router.get(
    "/logs",
    response_model=AuditLogListResponse,
    dependencies=[Depends(require_roles(*_HQ_ROLES, "store_owner"))],
)
async def list_audit_logs(
    domain: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    service: AuditService = Depends(get_audit_service),
) -> AuditLogListResponse:
    """감사 로그를 반환합니다. hq_admin·hq_operator·store_owner 역할에서 접근 가능합니다."""
    return await service.list_logs(domain=domain, limit=limit)
