from typing import Optional

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
    sku_id: Optional[str] = None
    ingredient_id: Optional[str] = None


class ProductionOverviewResponse(BaseModel):
    updated_at: str
    refresh_interval_minutes: int
    summary_stats: list[ProductionSummaryStat]
    alerts: list[ProductionOverviewAlert]


class ProductionSkuDecision(BaseModel):
    risk_level_label: str
    sales_velocity: float
    tags: list[str]
    alert_message: str
    can_produce: bool
    predicted_stockout_time: Optional[str] = None
    suggested_production_qty: int
    chance_loss_prevented_amount: Optional[int] = None


class ProductionSkuItem(BaseModel):
    sku_id: str
    sku_name: str
    current_stock: int
    forecast_stock_1h: int
    avg_first_production_qty_4w: int
    avg_first_production_time_4w: str
    avg_second_production_qty_4w: int
    avg_second_production_time_4w: str
    status: str
    chance_loss_saving_pct: int
    speed_alert: Optional[bool] = False
    speed_alert_message: Optional[str] = None
    material_alert: Optional[bool] = False
    material_alert_message: Optional[str] = None
    depletion_eta_minutes: Optional[int] = None
    recommended_production_qty: int
    chance_loss_basis_text: str
    decision: ProductionSkuDecision


class Pagination(BaseModel):
    page: int
    page_size: int
    total_items: int
    total_pages: int


class GetProductionSkuListResponse(BaseModel):
    items: list[ProductionSkuItem]
    pagination: Pagination


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
