from pydantic import BaseModel


class SalesSignal(BaseModel):
    id: str
    title: str
    metric: str
    value: str
    change: str
    trend: str
    priority: str
    region: str
    insight: str


class SignalsResponse(BaseModel):
    items: list[SalesSignal]
    high_count: int
