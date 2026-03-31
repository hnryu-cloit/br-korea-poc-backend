from fastapi import APIRouter

from app.schemas.contracts import (
    BootstrapResponse,
    ChannelDraft,
    HealthResponse,
    ReviewChecklistItem,
    SimulationInput,
    SimulationResponse,
)
from app.services.planning_service import PlanningService


router = APIRouter()
service = PlanningService()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/api/bootstrap", response_model=BootstrapResponse)
def bootstrap() -> BootstrapResponse:
    return BootstrapResponse(**service.get_bootstrap())


@router.post("/api/simulation/preview", response_model=SimulationResponse)
def simulate(payload: SimulationInput) -> SimulationResponse:
    return service.simulate(payload)


@router.get("/api/channels/drafts", response_model=dict[str, ChannelDraft])
def channel_drafts() -> dict[str, ChannelDraft]:
    return {
        key: ChannelDraft(**value)
        for key, value in service.get_bootstrap().get("channelDrafts", {}).items()
    }


@router.get("/api/review/checklist", response_model=list[ReviewChecklistItem])
def review_checklist() -> list[ReviewChecklistItem]:
    return [ReviewChecklistItem(**item) for item in service.get_bootstrap().get("reviewQueue", [])]
