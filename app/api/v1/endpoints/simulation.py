from fastapi import APIRouter

from app.schemas.simulation import SimulationInput, SimulationResponse
from app.services.planning_service import PlanningService

router = APIRouter(prefix="/simulation", tags=["simulation"])
service = PlanningService()


@router.post("/preview", response_model=SimulationResponse)
async def preview_simulation(payload: SimulationInput) -> SimulationResponse:
    return await service.simulate(payload)