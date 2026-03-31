from fastapi import APIRouter

from app.schemas.channels import ChannelDraft
from app.services.planning_service import PlanningService

router = APIRouter(prefix="/channels", tags=["channels"])
service = PlanningService()


@router.get("/drafts", response_model=dict[str, ChannelDraft])
async def list_channel_drafts() -> dict[str, ChannelDraft]:
    data = await service.get_bootstrap()
    return {
        key: ChannelDraft(**value)
        for key, value in data.get("channelDrafts", {}).items()
    }