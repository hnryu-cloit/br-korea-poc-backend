from typing import Any, Literal, Optional

from pydantic import BaseModel


class HomeOverviewRequest(BaseModel):
    store_id: Optional[str] = None
    business_date: Optional[str] = None


class HomeCta(BaseModel):
    label: str
    path: str


class HomePriorityActionBasisData(BaseModel):
    selection_rule: str
    sku_id: Optional[str] = None
    name: Optional[str] = None
    current: Optional[int] = None
    forecast: Optional[int] = None
    recommended: Optional[int] = None
    depletion_time: Optional[str] = None
    summary_status: Optional[str] = None
    recent_selection_count_7d: Optional[int] = None
    total: Optional[int] = None


class HomePriorityAction(BaseModel):
    id: str
    type: Literal["production", "ordering", "sales"]
    urgency: Literal["urgent", "important", "recommended"]
    badge_label: str
    title: str
    description: str
    cta: HomeCta
    focus_section: Optional[str] = None
    related_sku_id: Optional[str] = None
    ai_reasoning: Optional[str] = None
    confidence_score: Optional[float] = None
    is_finished_good: bool = False
    basis_data: Optional[HomePriorityActionBasisData] = None


class HomeStatItem(BaseModel):
    key: Literal["production_risk_count", "ordering_deadline_minutes", "today_profit_estimate", "alert_count"]
    label: str
    value: int | str
    unit: Optional[Literal["count", "minutes"]] = None
    tone: Literal["danger", "primary", "success", "default"]


class HomeCardMetric(BaseModel):
    key: str
    label: str
    value: int | str
    unit: Optional[Literal["count", "minutes"]] = None
    tone: Literal["danger", "primary", "success", "default"] = "default"


class HomeSummaryCard(BaseModel):
    domain: Literal["production", "ordering", "sales"]
    title: str
    description: str
    highlights_text: list[str]
    highlights_data: list[dict[str, Any]]
    metrics: list[HomeCardMetric]
    cta: HomeCta
    prompts: list[str]
    status_label: Optional[str] = None
    deadline_minutes: Optional[int] = None
    delivery_scheduled: Optional[bool] = None


class HomeOverviewResponse(BaseModel):
    updated_at: str
    priority_actions: list[HomePriorityAction]
    stats: list[HomeStatItem]
    cards: list[HomeSummaryCard]
