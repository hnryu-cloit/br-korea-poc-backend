from fastapi import APIRouter, Depends

from app.core.deps import get_bootstrap_service
from app.schemas.channels import ChannelDraft
from app.services.bootstrap_service import BootstrapService

router = APIRouter(prefix="/channels", tags=["channels"])


@router.get("/drafts", response_model=dict[str, ChannelDraft])
async def list_channel_drafts(
    service: BootstrapService = Depends(get_bootstrap_service),
) -> dict[str, ChannelDraft]:
    data = await service.get_channel_drafts()
    return {key: ChannelDraft(**value) for key, value in data.items()}
