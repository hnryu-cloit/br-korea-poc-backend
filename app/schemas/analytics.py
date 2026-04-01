from pydantic import BaseModel


class AnalyticsMetric(BaseModel):
    label: str
    value: str
    change: str
    trend: str
    detail: str


class AnalyticsMetricsResponse(BaseModel):
    items: list[AnalyticsMetric]
