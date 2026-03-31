from fastapi import APIRouter, Depends

from app.core.deps import get_bootstrap_service
from app.schemas.review import ReviewChecklistItem
from app.services.bootstrap_service import BootstrapService

router = APIRouter(prefix="/review", tags=["review"])


@router.get("/checklist", response_model=list[ReviewChecklistItem])
async def list_review_checklist(
    service: BootstrapService = Depends(get_bootstrap_service),
) -> list[ReviewChecklistItem]:
    data = await service.get_review_checklist()
    return [ReviewChecklistItem(**item) for item in data]
