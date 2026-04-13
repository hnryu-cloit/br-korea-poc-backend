from typing import Literal, Optional

from pydantic import BaseModel


class HomeOverviewRequest(BaseModel):
    store_id: Optional[str] = None
    business_date: Optional[str] = None


class HomePriorityAction(BaseModel):
    id: str
    type: Literal["production", "ordering", "sales"]
    urgency: Literal["urgent", "important", "recommended"]
    badge_label: str
    title: str
    description: str
    cta_label: str
    cta_path: str
    focus_section: Optional[str] = None
    related_sku_id: Optional[str] = None
    ai_reasoning: Optional[str] = None
    confidence_score: Optional[float] = None
    is_finished_good: bool = False


class HomeStatItem(BaseModel):
    key: Literal["production_risk_count", "ordering_deadline_minutes", "today_profit_estimate", "alert_count"]
    label: str
    value: str
    tone: Literal["danger", "primary", "success", "default"]


class HomeCardMetric(BaseModel):
    label: str
    value: str
    tone: Literal["danger", "primary", "success", "default"] = "default"


class HomeSummaryCard(BaseModel):
    domain: Literal["production", "ordering", "sales"]
    title: str
    description: str
    highlights: list[str]
    metrics: list[HomeCardMetric]
    cta_label: str
    cta_path: str
    prompts: list[str]
    status_label: Optional[str] = None
    deadline_minutes: Optional[int] = None
    delivery_scheduled: Optional[bool] = None


class HomeOverviewResponse(BaseModel):
    updated_at: str
    priority_actions: list[HomePriorityAction]
    stats: list[HomeStatItem]
    cards: list[HomeSummaryCard]
