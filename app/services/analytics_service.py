from __future__ import annotations

from app.repositories.analytics_repository import AnalyticsRepository
from app.schemas.analytics import AnalyticsMetric, AnalyticsMetricsResponse


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository) -> None:
        self.repository = repository

    async def get_metrics(self) -> AnalyticsMetricsResponse:
        items = await self.repository.get_metrics()
        return AnalyticsMetricsResponse(items=[AnalyticsMetric(**item) for item in items])
