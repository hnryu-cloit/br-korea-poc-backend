from fastapi import APIRouter, Depends

from app.core.deps import get_ordering_service
from app.schemas.simulation import SimulationInput, SimulationResponse
from app.services.ordering_service import OrderingService

router = APIRouter(prefix="/simulation", tags=["simulation"])


@router.post("/preview", response_model=SimulationResponse)
async def preview_simulation(
    payload: SimulationInput,
    service: OrderingService = Depends(get_ordering_service),
) -> SimulationResponse:
    return await service.simulate(payload)
