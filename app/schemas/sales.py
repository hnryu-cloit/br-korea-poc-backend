from typing import Optional

from pydantic import BaseModel


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
    comparison: Optional[SalesComparison] = None
