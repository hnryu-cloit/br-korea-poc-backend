from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str


class BootstrapResponse(BaseModel):
    product: str
    summary: str
    users: list[str]
    goals: list[str]
    policies: list[str]
    features: dict[str, list[dict[str, str]]]


class SimulationInput(BaseModel):
    promotion_name: str
    promo_price: float = Field(gt=0)
    list_price: float = Field(gt=0)
    procedure_cost: float = Field(ge=0)
    expected_leads: int = Field(ge=0)
    close_rate: float = Field(ge=0, le=1)
    upsell_rate: float = Field(ge=0, le=1)
    average_upsell_revenue: float = Field(ge=0)
    repeat_visit_rate: float = Field(ge=0, le=1)
    repeat_visit_revenue: float = Field(ge=0)
    ad_budget: float = Field(ge=0)


class SimulationResponse(BaseModel):
    promotion_name: str
    expected_patients: float
    expected_revenue: float
    expected_cost: float
    projected_profit: float
    break_even_patients: float
    allowed_ad_budget: float
    breakeven_reached: bool


class ReviewChecklistItem(BaseModel):
    stage: str
    owner: str
    status: str
    notes: str


class ChannelDraft(BaseModel):
    format: str
    headline: str
    body: str
    cta: str
