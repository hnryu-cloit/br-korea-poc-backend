from typing import Any, Optional

from pydantic import BaseModel


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


class ProductionOverviewResponse(BaseModel):
    updated_at: str
    production_lead_time_minutes: int
    danger_count: int
    items: list[ProductionItem]


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
    store_id: Optional[str] = None


class ProductionRegistrationResponse(BaseModel):
    sku_id: str
    qty: int
    registered_by: str
    feedback_type: str
    feedback_message: str
    store_id: Optional[str] = None


class ProductionRegistrationHistoryItem(BaseModel):
    sku_id: str
    qty: int
    registered_by: str
    feedback_type: str
    feedback_message: str
    registered_at: str
    store_id: Optional[str] = None


class ProductionRegistrationHistoryResponse(BaseModel):
    items: list[ProductionRegistrationHistoryItem]
    total: int
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None


class ProductionRegistrationSummaryResponse(BaseModel):
    total: int
    latest: Optional[ProductionRegistrationHistoryItem] = None
    total_registered_qty: int
    recent_registered_by: list[str]
    recent_registration_count_7d: int
    recent_registered_qty_7d: int
    affected_sku_count: int
    summary_status: str
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None


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
    chance_loss_reduction: Optional[float] = None


class ProductionSimulationResponse(BaseModel):
    metadata: dict[str, Any]
    summary_metrics: SimulationSummaryMetrics
    time_series_data: list[SimulationChartPoint]
    actions_timeline: list[str]
