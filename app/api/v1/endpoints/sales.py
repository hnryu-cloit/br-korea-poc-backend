from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query

from app.core.auth import get_current_role
from app.core.reference_datetime import resolve_date_range_by_reference
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
from app.services.explainability_service import (
    build_trace_id,
    create_pending_payload,
)

router = APIRouter(prefix="/sales", tags=["sales"])


@router.get("/prompts", response_model=list[SalesPrompt])
async def list_sales_prompts(
    domain: str = Query(default="sales"),
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: SalesService = Depends(get_sales_service),
) -> list[SalesPrompt]:
    try:
        return await service.list_prompts(
            domain=domain, store_id=store_id, date_from=date_from, date_to=date_to
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"추천 질문 조회 오류: {str(exc)}") from exc


@router.post("/query", response_model=SalesQueryResponse)
async def query_sales(
    payload: SalesQueryRequest,
    background_tasks: BackgroundTasks,
    role: str = Depends(get_current_role),
    x_store_id: str | None = Header(default=None, alias="X-Store-Id"),
    service: SalesService = Depends(get_sales_service),
) -> SalesQueryResponse:
    if not payload.store_id and x_store_id:
        payload.store_id = x_store_id
    result = await service.query(payload, actor_role=role)
    trace_id = build_trace_id("sales")
    pending = create_pending_payload(
        trace_id,
        actions=result.actions,
        evidence=result.evidence,
    )
    result.explainability = pending
    background_tasks.add_task(
        service.enrich_sales_query_explainability,
        trace_id=trace_id,
        store_id=payload.store_id,
        prompt=payload.prompt,
        base_text=result.text,
        base_actions=result.actions,
        base_evidence=result.evidence,
    )
    return result


@router.get("/insights", response_model=SalesInsightsResponse)
async def get_sales_insights(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: SalesService = Depends(get_sales_service),
) -> SalesInsightsResponse:
    try:
        resolved_date_from, resolved_date_to = resolve_date_range_by_reference(
            x_reference_datetime, date_from, date_to
        )
        return await service.get_insights(
            store_id=store_id,
            date_from=resolved_date_from,
            date_to=resolved_date_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"매출 인사이트 조회 오류: {str(exc)}") from exc


@router.get("/summary", response_model=SalesSummaryResponse)
async def get_sales_summary(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: SalesService = Depends(get_sales_service),
) -> SalesSummaryResponse:
    try:
        resolved_date_from, resolved_date_to = resolve_date_range_by_reference(
            x_reference_datetime, date_from, date_to
        )
        return await service.get_summary(
            store_id=store_id,
            date_from=resolved_date_from,
            date_to=resolved_date_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"매출 요약 조회 오류: {str(exc)}") from exc


@router.get("/campaign-effect", response_model=SalesCampaignEffectResponse)
async def get_sales_campaign_effect(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: SalesService = Depends(get_sales_service),
) -> SalesCampaignEffectResponse:
    try:
        resolved_date_from, resolved_date_to = resolve_date_range_by_reference(
            x_reference_datetime, date_from, date_to
        )
        return await service.get_campaign_effect(
            store_id=store_id, date_from=resolved_date_from, date_to=resolved_date_to
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"캠페인 효과 조회 오류: {str(exc)}") from exc
