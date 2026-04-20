from fastapi import APIRouter, Depends, Query

from app.core.auth import get_current_role
from app.core.deps import get_sales_service
from app.schemas.sales import (
    SalesCampaignEffectResponse,
    SalesInsightsResponse,
    SalesPrompt,
    SalesQueryRequest,
    SalesQueryResponse,
    SalesSummaryResponse,
)
from app.services.sales_service import SalesService

router = APIRouter(prefix="/sales", tags=["sales"])


@router.get("/prompts", response_model=list[SalesPrompt])
async def list_sales_prompts(
    domain: str = Query(default="sales"),
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: SalesService = Depends(get_sales_service),
) -> list[SalesPrompt]:
    return await service.list_prompts(
        domain=domain, store_id=store_id, date_from=date_from, date_to=date_to
    )


@router.post("/query", response_model=SalesQueryResponse)
async def query_sales(
    payload: SalesQueryRequest,
    role: str = Depends(get_current_role),
    service: SalesService = Depends(get_sales_service),
) -> SalesQueryResponse:
    return await service.query(payload, actor_role=role)


@router.get("/insights", response_model=SalesInsightsResponse)
async def get_sales_insights(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: SalesService = Depends(get_sales_service),
) -> SalesInsightsResponse:
    return await service.get_insights(store_id=store_id, date_from=date_from, date_to=date_to)


@router.get("/summary", response_model=SalesSummaryResponse)
async def get_sales_summary(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: SalesService = Depends(get_sales_service),
) -> SalesSummaryResponse:
    return await service.get_summary(store_id=store_id, date_from=date_from, date_to=date_to)


@router.get("/campaign-effect", response_model=SalesCampaignEffectResponse)
async def get_sales_campaign_effect(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: SalesService = Depends(get_sales_service),
) -> SalesCampaignEffectResponse:
    return await service.get_campaign_effect(
        store_id=store_id, date_from=date_from, date_to=date_to
    )
