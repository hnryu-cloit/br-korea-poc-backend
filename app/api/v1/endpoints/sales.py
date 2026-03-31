from fastapi import APIRouter, Depends

from app.core.auth import get_current_role
from app.core.deps import get_sales_service
from app.schemas.sales import SalesPrompt, SalesQueryRequest, SalesQueryResponse
from app.services.sales_service import SalesService

router = APIRouter(prefix="/sales", tags=["sales"])


@router.get("/prompts", response_model=list[SalesPrompt])
async def list_sales_prompts(
    service: SalesService = Depends(get_sales_service),
) -> list[SalesPrompt]:
    return await service.list_prompts()


@router.post("/query", response_model=SalesQueryResponse)
async def query_sales(
    payload: SalesQueryRequest,
    role: str = Depends(get_current_role),
    service: SalesService = Depends(get_sales_service),
) -> SalesQueryResponse:
    return await service.query(payload, actor_role=role)
