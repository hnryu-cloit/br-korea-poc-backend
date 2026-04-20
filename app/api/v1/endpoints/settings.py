from fastapi import APIRouter, Depends

from app.core.auth import require_roles
from app.core.deps import get_prompt_settings_service
from app.schemas.prompt_settings import PromptSettingsResponse, PromptSettingsUpdateRequest
from app.services.prompt_settings_service import PromptSettingsService

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/prompt", response_model=PromptSettingsResponse)
async def get_prompt_settings(
    _: str = Depends(require_roles("hq_admin")),
    service: PromptSettingsService = Depends(get_prompt_settings_service),
) -> PromptSettingsResponse:
    return service.get_settings()


@router.put("/prompt", response_model=PromptSettingsResponse)
async def update_prompt_settings(
    payload: PromptSettingsUpdateRequest,
    _: str = Depends(require_roles("hq_admin")),
    service: PromptSettingsService = Depends(get_prompt_settings_service),
) -> PromptSettingsResponse:
    return service.update_settings(payload)
