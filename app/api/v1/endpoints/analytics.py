from __future__ import annotations

from fastapi import APIRouter, Depends

from app.core.deps import get_analytics_service
from app.schemas.analytics import AnalyticsMetricsResponse
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/metrics", response_model=AnalyticsMetricsResponse)
async def get_analytics_metrics(
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnalyticsMetricsResponse:
    return await service.get_metrics()
