from __future__ import annotations

from typing import Literal
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response

from app.core.deps import get_analytics_service
from app.schemas.analytics import (
    AnalyticsMetricsResponse,
    CustomerProfileResponse,
    MarketIntelligenceResponse,
    SalesTrendResponse,
    StoreProfileResponse,
    WeatherImpactResponse,
)
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/metrics", response_model=AnalyticsMetricsResponse)
async def get_analytics_metrics(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnalyticsMetricsResponse:
    return await service.get_metrics(store_id=store_id, date_from=date_from, date_to=date_to)


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
    service: AnalyticsService = Depends(get_analytics_service),
) -> SalesTrendResponse:
    return service.get_sales_trend(store_id=store_id)


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


@router.get("/weather-impact", response_model=WeatherImpactResponse)
async def get_weather_impact(
    store_id: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    service: AnalyticsService = Depends(get_analytics_service),
) -> WeatherImpactResponse:
    return await service.get_weather_impact(store_id=store_id, date_from=date_from, date_to=date_to)


@router.get("/market-intelligence/weekly-report")
def download_market_weekly_report(
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
