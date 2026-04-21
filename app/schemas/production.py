from typing import Any

from pydantic import AliasChoices, BaseModel, Field


class ProductionItem(BaseModel):
    sku_id: str
    name: str
    current: int
    forecast: int
    status: str
    depletion_time: str
    recommended: int
    prod1: str
    prod2: str


class ProductionSummaryStat(BaseModel):
    key: str
    label: str
    value: str
    tone: str


class ProductionOverviewAlert(BaseModel):
    id: str
    type: str
    severity: str
    title: str
    description: str
    sku_id: str | None = None
    ingredient_id: str | None = None


class ProductionOverviewResponse(BaseModel):
    updated_at: str
    refresh_interval_minutes: int
    summary_stats: list[ProductionSummaryStat]
    alerts: list[ProductionOverviewAlert]
    production_lead_time_minutes: int
    danger_count: int
    items: list[ProductionItem]


class ProductionSkuDecision(BaseModel):
    risk_level_label: str
    sales_velocity: float
    tags: list[str]
    alert_message: str
    can_produce: bool
    predicted_stockout_time: str | None = None
    suggested_production_qty: int
    chance_loss_prevented_amount: int | None = None


class ProductionSkuItem(BaseModel):
    sku_id: str
    sku_name: str
    image_url: str | None = None
    current_stock: int
    forecast_stock_1h: int
    avg_first_production_qty_4w: int
    avg_first_production_time_4w: str
    avg_second_production_qty_4w: int
    avg_second_production_time_4w: str
    status: str
    chance_loss_saving_pct: int
    speed_alert: bool | None = False
    speed_alert_message: str | None = None
    material_alert: bool | None = False
    material_alert_message: str | None = None
    depletion_eta_minutes: int | None = None
    recommended_production_qty: int
    chance_loss_basis_text: str
    decision: ProductionSkuDecision
    predicted_stockout_time: str | None = None
    can_produce: bool | None = True
    sales_velocity: float | None = None
    tags: list[str] | None = Field(default_factory=list)
    alert_message: str | None = None


class Pagination(BaseModel):
    page: int
    page_size: int
    total_items: int
    total_pages: int


class GetProductionSkuListResponse(BaseModel):
    items: list[ProductionSkuItem]
    pagination: Pagination


class ProductionSkuDetailResponse(BaseModel):
    sku_id: str
    sku_name: str
    image_url: str | None = None
    current_stock: int
    forecast_stock_1h: int
    recommended_qty: int
    chance_loss_saving_pct: int
    chance_loss_basis_text: str
    predicted_stockout_time: str | None = None
    can_produce: bool | None = None
    sales_velocity: float | None = None
    tags: list[str] = Field(default_factory=list)
    alert_message: str | None = None
    material_alert: bool | None = False
    material_alert_message: str | None = None


class ProductionAlertItem(BaseModel):
    sku_id: str
    name: str
    current: int
    forecast: int
    depletion_time: str
    recommended: int
    prod1: str
    prod2: str
    severity: str
    push_title: str
    push_message: str
    target_roles: list[str]


class ProductionAlertsResponse(BaseModel):
    generated_at: str
    lead_time_minutes: int
    alerts: list[ProductionAlertItem]


class ProductionRegistrationRequest(BaseModel):
    sku_id: str
    qty: int
    registered_by: str = "store_owner"
    store_id: str | None = None


class ProductionRegistrationResponse(BaseModel):
    sku_id: str
    qty: int
    registered_by: str
    feedback_type: str
    feedback_message: str
    store_id: str | None = None


class ProductionRegistrationHistoryItem(BaseModel):
    sku_id: str
    qty: int
    registered_by: str
    feedback_type: str
    feedback_message: str
    registered_at: str
    store_id: str | None = None


class ProductionRegistrationHistoryResponse(BaseModel):
    items: list[ProductionRegistrationHistoryItem]
    total: int
    filtered_store_id: str | None = None
    filtered_date_from: str | None = None
    filtered_date_to: str | None = None


class ProductionRegistrationSummaryResponse(BaseModel):
    total: int
    latest: ProductionRegistrationHistoryItem | None = None
    total_registered_qty: int
    recent_registered_by: list[str]
    recent_registration_count_7d: int
    recent_registered_qty_7d: int
    affected_sku_count: int
    summary_status: str
    filtered_store_id: str | None = None
    filtered_date_from: str | None = None
    filtered_date_to: str | None = None


class ProductionSimulationRequest(BaseModel):
    store_id: str
    item_id: str
    simulation_date: str  # YYYY-MM-DD
    lead_time_hour: int = 1
    margin_rate: float = 0.3


class SimulationChartPoint(BaseModel):
    time: str
    actual_stock: float
    ai_guided_stock: float


class SimulationSummaryMetrics(BaseModel):
    additional_sales_qty: float
    additional_profit_amt: int
    additional_waste_qty: float
    additional_waste_cost: int
    net_profit_change: int
    performance_status: str
    chance_loss_reduction: float | None = None


class ProductionSimulationResponse(BaseModel):
    metadata: dict[str, Any]
    summary_metrics: SimulationSummaryMetrics
    time_series_data: list[SimulationChartPoint] = Field(
        validation_alias=AliasChoices("time_series_data", "chart_data")
    )
    actions_timeline: list[str] = Field(
        validation_alias=AliasChoices("actions_timeline", "action_timeline")
    )


class ProductionRegistrationFormItem(BaseModel):
    sku_id: str
    sku_name: str
    recommended_qty: int
    current_stock: int
    forecast_stock_1h: int
    basis_text: str
    last_registered_at: str | None = None
    last_registered_qty: int | None = None


class ProductionRegistrationFormResponse(BaseModel):
    items: list[ProductionRegistrationFormItem]
    generated_at: str


# AI 계약과의 호환성을 위한 별칭
SimulationRequest = ProductionSimulationRequest
SimulationReportResponse = ProductionSimulationResponse
SimulationSummary = SimulationSummaryMetrics
ChartDataPoint = SimulationChartPoint


class WasteItem(BaseModel):
    item_nm: str
    total_disuse_qty: float
    loss_amount: float


class WasteSummaryResponse(BaseModel):
    items: list[WasteItem]
    total_loss_amount: float


class InventoryStatusItem(BaseModel):
    item_nm: str
    total_stock: float
    total_sold: float
    status: str


class InventoryStatusResponse(BaseModel):
    items: list[InventoryStatusItem]
    pagination: Pagination
