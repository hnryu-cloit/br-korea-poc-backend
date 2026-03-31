from fastapi import APIRouter

from app.schemas.review import ReviewChecklistItem
from app.services.planning_service import PlanningService

router = APIRouter(prefix="/review", tags=["review"])
service = PlanningService()


@router.get("/checklist", response_model=list[ReviewChecklistItem])
async def list_review_checklist() -> list[ReviewChecklistItem]:
    data = await service.get_bootstrap()
    return [ReviewChecklistItem(**item) for item in data.get("reviewQueue", [])]