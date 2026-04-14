from typing import Optional

from pydantic import BaseModel, Field


class SalesPrompt(BaseModel):
    label: str
    category: str
    prompt: str


class SalesComparisonMetric(BaseModel):
    label: str
    store_value: str
    peer_value: str


class SalesComparison(BaseModel):
    store: str
    peer_group: str
    summary: str
    metrics: list[SalesComparisonMetric]


class SalesQueryRequest(BaseModel):
    prompt: str


class SalesQueryResponse(BaseModel):
    text: str
    evidence: list[str]
    actions: list[str]
    store_context: Optional[str] = ""
    data_source: Optional[str] = ""
    comparison_basis: Optional[str] = ""
    calculation_date: Optional[str] = ""
    comparison: Optional[SalesComparison] = None
    query_type: Optional[str] = None
    processing_route: Optional[str] = None
    blocked: bool = False
    masked_fields: list[str] = Field(default_factory=list)
    confidence_score: Optional[float] = 1.0
    semantic_logic: Optional[str] = None
    sources: Optional[list[str]] = None
    visual_data: Optional[dict] = None


class SalesInsightMetric(BaseModel):
    label: str
    value: str
    detail: Optional[str] = None


class SalesInsightSection(BaseModel):
    title: str
    summary: str
    metrics: list[SalesInsightMetric]
    actions: list[str]
    status: str = "normal"


class SalesInsightsResponse(BaseModel):
    peak_hours: SalesInsightSection
    channel_mix: SalesInsightSection
    payment_mix: SalesInsightSection
    menu_mix: SalesInsightSection
    campaign_seasonality: Optional[SalesInsightSection] = None
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None
