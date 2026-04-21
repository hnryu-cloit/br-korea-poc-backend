from pydantic import BaseModel


class OrderingOptionItemLine(BaseModel):
    sku_id: str | None = None
    sku_name: str
    quantity: int
    note: str | None = None


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


class OrderingWeather(BaseModel):
    region: str
    forecast_date: str
    weather_type: str
    max_temperature_c: int | None = None
    min_temperature_c: int | None = None
    precipitation_probability: int | None = None


class OrderingOptionsResponse(BaseModel):
    deadline_minutes: int
    deadline_at: str | None = None
    notification_entry: bool = False
    purpose_text: str = "주문 누락을 방지하고 최적 수량을 선택하세요."
    caution_text: str = "최종 주문 결정은 점주 권한입니다. 추천 옵션은 보조 자료로만 활용해주세요."
    weather: OrderingWeather | None = None
    trend_summary: str | None = None
    business_date: str | None = None
    options: list[OrderOption]


class OrderingContextResponse(BaseModel):
    notification_id: int
    target_path: str
    focus_option_id: str | None = None
    message: str


class OrderingDeadlineAlert(BaseModel):
    notification_id: int
    title: str
    message: str
    deadline_minutes: int
    target_path: str
    focus_option_id: str | None = None
    target_roles: list[str]


class OrderingAlertsResponse(BaseModel):
    generated_at: str
    alerts: list[OrderingDeadlineAlert]


class OrderSelectionRequest(BaseModel):
    option_id: str
    reason: str | None = None
    actor_role: str = "store_owner"
    store_id: str | None = None


class OrderSelectionResponse(BaseModel):
    selection_id: str | None = None
    option_id: str
    reason: str | None = None
    saved: bool


class OrderSelectionHistoryItem(BaseModel):
    selection_id: str | None = None
    option_id: str
    option_title: str | None = None
    actor_role: str
    store_id: str | None = None
    reason: str | None = None
    selected_at: str


class OrderSelectionHistoryResponse(BaseModel):
    items: list[OrderSelectionHistoryItem]
    total: int
    filtered_store_id: str | None = None
    filtered_date_from: str | None = None
    filtered_date_to: str | None = None


class OrderSelectionSummaryResponse(BaseModel):
    total: int
    latest: OrderSelectionHistoryItem | None = None
    recommended_selected: bool
    recent_actor_roles: list[str]
    recent_selection_count_7d: int
    option_counts: dict[str, int]
    summary_status: str
    filtered_store_id: str | None = None
    filtered_date_from: str | None = None
    filtered_date_to: str | None = None


class OrderingHistoryItem(BaseModel):
    item_nm: str
    dlv_dt: str | None
    ord_qty: int | None
    confrm_qty: int | None
    is_auto: bool
    ord_grp_nm: str | None


class OrderingHistoryResponse(BaseModel):
    items: list[OrderingHistoryItem]
    auto_rate: float
    manual_rate: float
    total_count: int


class OrderingHistoryInsightKpi(BaseModel):
    key: str
    label: str
    value: str
    tone: str = "default"


class OrderingHistoryAnomalyItem(BaseModel):
    id: str
    severity: str
    kind: str
    message: str
    recommended_action: str
    related_items: list[str] = []


class OrderingHistoryChangedItem(BaseModel):
    item_nm: str
    avg_ord_qty: float
    latest_ord_qty: int
    change_ratio: float


class OrderingHistoryInsightsResponse(BaseModel):
    kpis: list[OrderingHistoryInsightKpi]
    anomalies: list[OrderingHistoryAnomalyItem]
    top_changed_items: list[OrderingHistoryChangedItem]
    sources: list[str] = []
    retrieved_contexts: list[str] = []
    confidence: float | None = None
