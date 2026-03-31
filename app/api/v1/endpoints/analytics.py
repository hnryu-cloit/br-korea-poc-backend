from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/analytics", tags=["analytics"])


class AnalyticsMetric(BaseModel):
    label: str
    value: str
    change: str
    trend: str
    detail: str


class AnalyticsMetricsResponse(BaseModel):
    items: list[AnalyticsMetric]


_STUB: list[AnalyticsMetric] = [
    AnalyticsMetric(label="이번 주 총 매출", value="₩4,382,000", change="+6.2%", trend="up", detail="지난주 대비"),
    AnalyticsMetric(label="배달 건수", value="312건", change="-14.3%", trend="down", detail="지난주 대비"),
    AnalyticsMetric(label="홀 방문 고객", value="487명", change="+3.1%", trend="up", detail="지난주 대비"),
    AnalyticsMetric(label="앱 주문 비중", value="28%", change="+0%", trend="flat", detail="지난주 대비"),
    AnalyticsMetric(label="커피 동반 구매율", value="62%", change="+8.4%", trend="up", detail="지난주 대비"),
    AnalyticsMetric(label="평균 객단가", value="₩8,940", change="+2.7%", trend="up", detail="지난주 대비"),
]


@router.get("/metrics", response_model=AnalyticsMetricsResponse)
async def get_analytics_metrics() -> AnalyticsMetricsResponse:
    return AnalyticsMetricsResponse(items=_STUB)