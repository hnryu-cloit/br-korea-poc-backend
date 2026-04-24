from __future__ import annotations

from typing import Literal
from urllib.parse import quote

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import Response

from app.core.auth import require_roles
from app.core.reference_datetime import resolve_date_range_by_reference
from app.core.deps import get_analytics_service
from app.schemas.analytics import (
    AnalyticsMetricsResponse,
    CustomerProfileResponse,
    HQMarketInsightsResponse,
    MarketIntelligenceResponse,
    MarketInsightsResponse,
    SalesTrendResponse,
    StoreProfileResponse,
    WeatherImpactResponse,
)
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["analytics"])
_HQ_ROLES = ("hq_admin", "hq_operator")


@router.get("/metrics", response_model=AnalyticsMetricsResponse)
async def get_analytics_metrics(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnalyticsMetricsResponse:
    try:
        resolved_date_from, resolved_date_to = resolve_date_range_by_reference(
            x_reference_datetime, date_from, date_to
        )
        return await service.get_metrics(
            store_id=store_id,
            date_from=resolved_date_from,
            date_to=resolved_date_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"매출 지표 조회 오류: {str(exc)}") from exc


@router.get("/store-profile", response_model=StoreProfileResponse)
def get_store_profile(
    store_id: str | None = Query(default=None),
    service: AnalyticsService = Depends(get_analytics_service),
) -> StoreProfileResponse:
    result = service.get_store_profile(store_id=store_id)
    if result is None:
        raise HTTPException(status_code=404, detail="매장 정보를 찾을 수 없습니다.")
    return result


@router.get("/customer-profile", response_model=CustomerProfileResponse)
def get_customer_profile(
    store_id: str | None = Query(default=None),
    service: AnalyticsService = Depends(get_analytics_service),
) -> CustomerProfileResponse:
    return service.get_customer_profile(store_id=store_id)


@router.get("/sales-trend", response_model=SalesTrendResponse)
def get_sales_trend(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    compare_mode: Literal["prev_week", "prev_month"] = Query(default="prev_month"),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: AnalyticsService = Depends(get_analytics_service),
) -> SalesTrendResponse:
    try:
        resolved_date_from, resolved_date_to = resolve_date_range_by_reference(
            x_reference_datetime, date_from, date_to
        )
        return service.get_sales_trend(
            store_id=store_id,
            date_from=resolved_date_from,
            date_to=resolved_date_to,
            compare_mode=compare_mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"매출 추이 조회 오류: {str(exc)}") from exc


@router.get("/market-intelligence", response_model=MarketIntelligenceResponse)
def get_market_intelligence(
    store_id: str | None = Query(default=None),
    gu: str | None = Query(default=None),
    dong: str | None = Query(default=None),
    industry: str | None = Query(default=None),
    year: int | None = Query(default=None),
    quarter: str | None = Query(default=None),
    radius_m: int | None = Query(default=None, ge=100, le=3000),
    service: AnalyticsService = Depends(get_analytics_service),
    ) -> MarketIntelligenceResponse:
    return service.get_market_intelligence(
        store_id=store_id,
        gu=gu,
        dong=dong,
        industry=industry,
        year=year,
        quarter=quarter,
        radius_m=radius_m,
    )


@router.get("/market-intelligence/insights", response_model=MarketInsightsResponse)
async def get_market_intelligence_insights(
    store_id: str | None = Query(default=None),
    gu: str | None = Query(default=None),
    dong: str | None = Query(default=None),
    industry: str | None = Query(default=None),
    year: int | None = Query(default=None),
    quarter: str | None = Query(default=None),
    radius_m: int | None = Query(default=None, ge=100, le=3000),
    service: AnalyticsService = Depends(get_analytics_service),
) -> MarketInsightsResponse:
    return await service.get_market_insights(
        store_id=store_id,
        gu=gu,
        dong=dong,
        industry=industry,
        year=year,
        quarter=quarter,
        radius_m=radius_m,
    )


@router.get(
    "/market-intelligence/insights/hq",
    response_model=HQMarketInsightsResponse,
    dependencies=[Depends(require_roles(*_HQ_ROLES))],
)
async def get_market_intelligence_insights_hq(
    gu: str | None = Query(default=None),
    dong: str | None = Query(default=None),
    industry: str | None = Query(default=None),
    year: int | None = Query(default=None),
    quarter: str | None = Query(default=None),
    radius_m: int | None = Query(default=None, ge=100, le=3000),
    limit: int = Query(default=20, ge=1, le=100),
    service: AnalyticsService = Depends(get_analytics_service),
) -> HQMarketInsightsResponse:
    return await service.get_hq_market_insights(
        gu=gu,
        dong=dong,
        industry=industry,
        year=year,
        quarter=quarter,
        radius_m=radius_m,
        limit=limit,
    )


@router.get("/weather-impact", response_model=WeatherImpactResponse)
async def get_weather_impact(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    x_reference_datetime: str | None = Header(default=None, alias="X-Reference-Datetime"),
    service: AnalyticsService = Depends(get_analytics_service),
) -> WeatherImpactResponse:
    resolved_date_from, resolved_date_to = resolve_date_range_by_reference(
        x_reference_datetime, date_from, date_to
    )
    return await service.get_weather_impact(
        store_id=store_id,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
    )


@router.get("/market-intelligence/weekly-report")
async def download_market_weekly_report(
    store_id: str | None = Query(default=None),
    gu: str | None = Query(default=None),
    dong: str | None = Query(default=None),
    industry: str | None = Query(default=None),
    year: int | None = Query(default=None),
    quarter: str | None = Query(default=None),
    radius_m: int | None = Query(default=None, ge=100, le=3000),
    format: Literal["md", "pdf"] = Query(default="md"),
    service: AnalyticsService = Depends(get_analytics_service),
) -> Response:
    filename, markdown = service.get_weekly_market_report_markdown(
        store_id=store_id,
        gu=gu,
        dong=dong,
        industry=industry,
        year=year,
        quarter=quarter,
        radius_m=radius_m,
    )
    insights = await service.get_market_insights(
        store_id=store_id,
        gu=gu,
        dong=dong,
        industry=industry,
        year=year,
        quarter=quarter,
        radius_m=radius_m,
    )
    if insights.report_markdown:
        markdown = insights.report_markdown
    if format == "pdf":
        pdf_filename, pdf_bytes = service.render_weekly_market_report_pdf(markdown, filename)
        encoded = quote(pdf_filename)
        headers = {
            "Content-Disposition": f"attachment; filename=\"weekly_market_report.pdf\"; filename*=UTF-8''{encoded}"
        }
        return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
    encoded = quote(filename)
    headers = {
        "Content-Disposition": f"attachment; filename=\"weekly_market_report.md\"; filename*=UTF-8''{encoded}"
    }
    return Response(content=markdown, media_type="text/markdown; charset=utf-8", headers=headers)
