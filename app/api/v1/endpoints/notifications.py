from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/notifications", tags=["notifications"])


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


_STUB: list[NotificationItem] = [
    NotificationItem(
        id=1,
        category="alert",
        title="생산 알림 발송 필요",
        description="스트로베리 필드 1시간 내 품절 위험이 감지되었습니다.",
        created_at="방금 전",
        unread=True,
        link_to="/production",
        link_state=None,
    ),
    NotificationItem(
        id=2,
        category="workflow",
        title="주문 추천 생성 완료",
        description="전주/전전주/전월 동요일 기준 3개 옵션이 준비되었습니다.",
        created_at="4분 전",
        unread=True,
        link_to="/ordering",
        link_state={"source": "notification", "notificationId": 2, "focusOptionId": "opt-a"},
    ),
    NotificationItem(
        id=3,
        category="analysis",
        title="매출 질의 응답 준비",
        description="배달 채널 감소 원인과 액션 아이템이 정리되었습니다.",
        created_at="18분 전",
        unread=False,
        link_to="/sales",
        link_state={"source": "notification", "notificationId": 3, "prompt": "이번 주 배달 건수가 지난주보다 줄어든 원인을 알려줘"},
    ),
]


@router.get("", response_model=NotificationListResponse)
async def list_notifications() -> NotificationListResponse:
    return NotificationListResponse(
        items=_STUB,
        unread_count=sum(1 for n in _STUB if n.unread),
    )
