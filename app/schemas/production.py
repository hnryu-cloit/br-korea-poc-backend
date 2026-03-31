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


class ProductionRegistrationRequest(BaseModel):
    sku_id: str
    qty: int
    registered_by: str = "store_owner"


class ProductionRegistrationResponse(BaseModel):
    sku_id: str
    qty: int
    registered_by: str
    feedback_type: str
    feedback_message: str
