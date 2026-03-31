from typing import Optional

from pydantic import BaseModel


class OrderItem(BaseModel):
    name: str
    qty: int
    note: Optional[str] = None


class OrderOption(BaseModel):
    id: str
    label: str
    basis: str
    description: str
    recommended: bool
    items: list[OrderItem]
    notes: list[str]


class OrderingOptionsResponse(BaseModel):
    deadline_minutes: int
    notification_entry: bool = False
    focus_option_id: Optional[str] = None
    options: list[OrderOption]


class OrderingContextResponse(BaseModel):
    notification_id: int
    target_path: str
    focus_option_id: Optional[str] = None
    message: str


class OrderingDeadlineAlert(BaseModel):
    notification_id: int
    title: str
    message: str
    deadline_minutes: int
    target_path: str
    focus_option_id: Optional[str] = None
    target_roles: list[str]


class OrderingAlertsResponse(BaseModel):
    generated_at: str
    alerts: list[OrderingDeadlineAlert]


class OrderSelectionRequest(BaseModel):
    option_id: str
    reason: Optional[str] = None
    actor: str = "store_owner"
    store_id: Optional[str] = None


class OrderSelectionResponse(BaseModel):
    option_id: str
    reason: Optional[str] = None
    actor: str
    saved: bool
    store_id: Optional[str] = None


class OrderSelectionHistoryItem(BaseModel):
    option_id: str
    reason: Optional[str] = None
    actor: str
    saved: bool
    selected_at: str
    store_id: Optional[str] = None


class OrderSelectionHistoryResponse(BaseModel):
    items: list[OrderSelectionHistoryItem]
    total: int
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None


class OrderSelectionSummaryResponse(BaseModel):
    total: int
    latest: Optional[OrderSelectionHistoryItem] = None
    recommended_selected: bool
    recent_actor_roles: list[str]
    recent_selection_count_7d: int
    option_counts: dict[str, int]
    summary_status: str
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None
