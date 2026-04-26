from datetime import date
from typing import Optional, Any

from pydantic import BaseModel, Field

from app.schemas.explainability import ExplainabilityPayload

class SalesPrompt(BaseModel):
    label: str
    category: str
    prompt: str


class SalesComparisonMetric(BaseModel):
    label: str
    store_value: str
    peer_value: str


class SalesComparison(BaseModel):
    store: str
    peer_group: str
    summary: str
    metrics: list[SalesComparisonMetric]


class SalesAnswer(BaseModel):
    text: str
    evidence: list[str] = Field(default_factory=list)
    actions: list[str] = Field(default_factory=list)
    follow_up_questions: list[str] = Field(default_factory=list)


class SalesQueryRequestContext(BaseModel):
    store_id: str
    business_date: str
    business_time: Optional[str] = None
    prompt: str
    domain: str = "sales"


class SalesQueryAgentTrace(BaseModel):
    keywords: list[str] = Field(default_factory=list)
    intent: Optional[str] = None
    relevant_tables: list[str] = Field(default_factory=list)
    sql: Optional[str] = None
    queried_period: Optional[dict[str, Any]] = None
    row_count: int = 0
    matched_query_id: Optional[str] = None
    match_score: Optional[float] = None
    overlap_candidates: list[dict[str, Any]] = Field(default_factory=list)


class ChatHistoryEntry(BaseModel):
    role: str
    text: str


class SalesQueryRequest(BaseModel):
    prompt: str
    store_id: str = Field(..., min_length=1)
    domain: str = Field(default="sales")
    business_date: str = Field(default_factory=lambda: date.today().isoformat())
    business_time: Optional[str] = None
    page_context: Optional[str] = None
    card_context_key: Optional[str] = None
    store_name: Optional[str] = None
    user_role: Optional[str] = None
    conversation_history: Optional[list[ChatHistoryEntry]] = None


class SalesQueryResponse(BaseModel):
    text: str
    evidence: list[str]
    actions: list[str]
    follow_up_questions: list[str] = Field(default_factory=list)
    answer: Optional[SalesAnswer] = None
    request_context: Optional[SalesQueryRequestContext] = None
    agent_trace: Optional[SalesQueryAgentTrace] = None
    store_context: Optional[str] = ""
    data_source: Optional[str] = ""
    comparison_basis: Optional[str] = ""
    calculation_date: Optional[str] = ""
    comparison: Optional[SalesComparison] = None
    query_type: Optional[str] = None
    processing_route: Optional[str] = None
    blocked: bool = False
    masked_fields: list[str] = Field(default_factory=list)
    confidence_score: Optional[float] = 1.0
    semantic_logic: Optional[str] = None
    sources: Optional[list[str]] = None
    visual_data: Optional[dict[str, Any]] = None
    data_lineage: Optional[list[dict[str, Any]]] = Field(default_factory=list, description="AI가 생성 및 실행한 쿼리 히스토리 (투명성 검증용)")
    explainability: ExplainabilityPayload | None = None



class SalesInsightMetric(BaseModel):
    label: str
    value: str
    detail: Optional[str] = None


class SalesInsightSection(BaseModel):
    title: str
    summary: str
    metrics: list[SalesInsightMetric]
    actions: list[str]
    status: str = "normal"


class SalesInsightsResponse(BaseModel):
    peak_hours: SalesInsightSection
    channel_mix: SalesInsightSection
    payment_mix: SalesInsightSection
    menu_mix: SalesInsightSection
    campaign_seasonality: Optional[SalesInsightSection] = None
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None
    explainability: ExplainabilityPayload | None = None


class MenuInsightsResponse(BaseModel):
    cards: list[SalesInsightSection] = Field(default_factory=list)
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None
    explainability: ExplainabilityPayload | None = None


class SalesWeeklyItem(BaseModel):
    day: str
    revenue: float
    net_revenue: float


class SalesProductItem(BaseModel):
    name: str
    sales: float
    qty: float


class SalesSummaryResponse(BaseModel):
    data_date: Optional[str] = None
    today_revenue: float = 0.0
    today_net_revenue: float = 0.0
    weekly_data: list[SalesWeeklyItem] = Field(default_factory=list)
    top_products: list[SalesProductItem] = Field(default_factory=list)
    avg_margin_rate: float = 0.0
    avg_net_profit_per_item: float = 0.0
    avg_ticket_size: float = 0.0
    avg_ticket_index: float = 0.0
    estimated_today_profit: float = 0.0
    explainability: ExplainabilityPayload | None = None


class SalesCampaignEffectResponse(BaseModel):
    title: str = "캠페인 효과 분석"
    summary: str = "캠페인 효과 데이터가 준비되지 않았습니다."
    metrics: list[SalesInsightMetric] = Field(default_factory=list)
    actions: list[str] = Field(default_factory=list)
    explainability: ExplainabilityPayload | None = None


class SalesHourlyChannelItem(BaseModel):
    hour: int  # 0~23 시간대
    offline_sales: float = 0.0  # 오프라인+투고 매출액
    delivery_sales: float = 0.0  # 배달 매출액
    offline_qty: float = 0.0  # 오프라인+투고 판매건수
    delivery_qty: float = 0.0  # 배달 판매건수
    total_qty: float = 0.0  # 전체 판매건수


class SalesHourlyChannelResponse(BaseModel):
    items: list[SalesHourlyChannelItem] = Field(default_factory=list)
    filtered_store_id: Optional[str] = None
    filtered_date_from: Optional[str] = None
    filtered_date_to: Optional[str] = None
    explainability: ExplainabilityPayload | None = None
