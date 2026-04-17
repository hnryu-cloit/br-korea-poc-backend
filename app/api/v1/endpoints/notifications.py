from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_notifications_service
from app.schemas.notifications import NotificationListResponse
from app.services.notifications_service import NotificationsService

router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.get("", response_model=NotificationListResponse)
async def list_notifications(
    store_id: Optional[str] = Query(default=None),
    service: NotificationsService = Depends(get_notifications_service),
) -> NotificationListResponse:
    return await service.list_notifications(store_id=store_id)
