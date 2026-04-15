from typing import Optional

from pydantic import BaseModel


class OrderingOptionItemLine(BaseModel):
    sku_id: Optional[str] = None
    sku_name: str
    quantity: int
    note: Optional[str] = None


class OrderingOptionMetric(BaseModel):
    key: str
    value: str


class OrderOption(BaseModel):
    option_id: str
    title: str
    basis: str
    description: str
    recommended: bool
    reasoning_text: str
    reasoning_metrics: list[OrderingOptionMetric]
    special_factors: list[str]
    items: list[OrderingOptionItemLine]


class OrderingOptionsResponse(BaseModel):
    deadline_minutes: int
    deadline_at: Optional[str] = None
    notification_entry: bool = False
    purpose_text: str = "주문 누락을 방지하고 최적 수량을 선택하세요."
    caution_text: str = "최종 주문 결정은 점주 권한입니다. 추천 옵션은 보조 자료로만 활용해주세요."
    weather_summary: Optional[str] = None
    trend_summary: Optional[str] = None
    business_date: Optional[str] = None
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
    actor_role: str = "store_owner"
    store_id: Optional[str] = None


class OrderSelectionResponse(BaseModel):
    selection_id: Optional[str] = None
    option_id: str
    reason: Optional[str] = None
    saved: bool


class OrderSelectionHistoryItem(BaseModel):
    selection_id: Optional[str] = None
    option_id: str
    option_title: Optional[str] = None
    actor_role: str
    store_id: Optional[str] = None
    reason: Optional[str] = None
    selected_at: str


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