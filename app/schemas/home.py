from typing import Any, Literal, Optional

from pydantic import BaseModel


class HomeOverviewRequest(BaseModel):
    store_id: str | None = None
    business_date: str | None = None


class HomeCta(BaseModel):
    label: str
    path: str


class HomePriorityActionBasisData(BaseModel):
    selection_rule: str
    sku_id: str | None = None
    name: str | None = None
    current: int | None = None
    forecast: int | None = None
    recommended: int | None = None
    depletion_time: str | None = None
    summary_status: str | None = None
    recent_selection_count_7d: int | None = None
    total: int | None = None


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
    basis_data: HomePriorityActionBasisData | None = None


class HomeStatItem(BaseModel):
    key: Literal["production_risk_count", "ordering_deadline_minutes", "today_profit_estimate", "alert_count"]
    label: str
    value: int | str
    unit: Literal["count", "minutes"] | None = None
    tone: Literal["danger", "primary", "success", "default"]


class HomeCardMetric(BaseModel):
    key: str
    label: str
    value: int | str
    unit: Literal["count", "minutes"] | None = None
    tone: Literal["danger", "primary", "success", "default"] = "default"


class HomeSummaryCard(BaseModel):
    domain: Literal["production", "ordering", "sales"]
    title: str
    description: str
    highlights: list[dict[str, Any]]
    metrics: list[HomeCardMetric]
    cta: HomeCta
    prompts: list[str]
    status_label: str | None = None
    deadline_minutes: int | None = None
    delivery_scheduled: bool | None = None


class HomeOrderingDeadline(BaseModel):
    supplier_name: str
    menu_type: str
    deadline_time: str
    remaining_minutes: int
    is_imminent: bool
    order_type: Literal["stock", "finished_good"]


class HomeOverviewResponse(BaseModel):
    updated_at: str
    stats: list[HomeStatItem]
    cards: list[HomeSummaryCard]
    imminent_deadlines: list[HomeOrderingDeadline]

class ScheduleEvent(BaseModel):
    date: str
    title: str
    category: str  # campaign | telecom | notice
    type: str
    startDate: str
    endDate: str


class ScheduleNotice(BaseModel):
    id: str
    title: str
    category: Literal["campaign", "telecom", "notice"]
    type: str
    startDate: str
    endDate: str
    tone: Literal["blue", "green", "orange", "rose"] = "blue"


class ScheduleTodoItem(BaseModel):
    id: str
    label: str
    recurring: bool = False


class ScheduleResponse(BaseModel):
    updated_at: str
    source: str
    events: list[ScheduleEvent]
    notices: list[ScheduleNotice] = []
    todos: list[ScheduleTodoItem] = []
