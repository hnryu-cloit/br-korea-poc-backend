from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from app.schemas.explainability import ExplainabilityPayload

class AnalyticsMetric(BaseModel):
    label: str
    value: str
    change: str
    trend: str
    detail: str


class AnalyticsMetricsResponse(BaseModel):
    items: list[AnalyticsMetric]
    selected_period_total_sales: float | None = None
    explainability: ExplainabilityPayload | None = None


class StoreProfileResponse(BaseModel):
    store_cd: str
    store_nm: str
    sido: str
    region: str
    store_type: str
    area_pyeong: int
    business_type: str
    peer_count: int
    actual_sales_amt: float


class CustomerSegmentItem(BaseModel):
    segment_nm: str
    count: int


class TelecomDiscountItem(BaseModel):
    name: str
    type_nm: str
    value: str
    method_nm: str


class CustomerProfileResponse(BaseModel):
    customer_segments: list[CustomerSegmentItem]
    telecom_discounts: list[TelecomDiscountItem]


class MarketScopeOptionsResponse(BaseModel):
    gu_options: list[str]
    dong_options_by_gu: dict[str, list[str]]


class SalesTrendPoint(BaseModel):
    day: int
    current_period: float | None = None
    comparison_period: float | None = None
    projection: float | None = None


class SalesTrendInsightChip(BaseModel):
    label: str
    value: str
    trend: str


class DowPoint(BaseModel):
    dow: int  # 0=월 ~ 6=일
    label: str  # 월·화·수·목·금·토·일
    current_period_avg: float
    comparison_period_avg: float


class HourPoint(BaseModel):
    hour: int
    current_period_avg: float
    comparison_period_avg: float


class SalesTrendResponse(BaseModel):
    headline: str
    headline_trend: str
    compare_mode: Literal["prev_week", "prev_month"]
    date_from: str
    date_to: str
    comparison_date_from: str | None = None
    comparison_date_to: str | None = None
    selected_period_total_sales: float
    comparison_period_total_sales: float
    points: list[SalesTrendPoint]
    insight_chips: list[SalesTrendInsightChip]
    dow_points: list[DowPoint]
    hour_points: list[HourPoint]
    explainability: ExplainabilityPayload | None = None


class WeatherImpactCorrelation(BaseModel):
    metric: str
    temperature_corr: float
    precipitation_corr: float


class WeatherImpactBySido(BaseModel):
    sido: str
    samples: int
    avg_temperature: float
    avg_precipitation: float
    correlations: list[WeatherImpactCorrelation]


class WeatherImpactResponse(BaseModel):
    date_from: str
    date_to: str
    items: list[WeatherImpactBySido]
    explainability: ExplainabilityPayload | None = None


class TradeAreaSalesSlice(BaseModel):
    category: str
    sales_amount: float
    share_ratio: float


class CompetitorTrendPoint(BaseModel):
    month: str
    sales_amount: float


class CompetitorPaymentDemographic(BaseModel):
    age_group: str
    male_payment_count: int
    female_payment_count: int


class CompetitorInsightItem(BaseModel):
    rank: int
    brand_name: str
    store_name: str
    distance_km: float
    trend_direction: str
    sales_trend: list[CompetitorTrendPoint]
    payment_demographics: list[CompetitorPaymentDemographic]


class FloatingPopulationTrendPoint(BaseModel):
    month: str
    floating_population: int
    estimated_sales_amount: float


class ResidentialPopulationRadarItem(BaseModel):
    age_group: str
    male_population: int
    female_population: int


class HouseholdCompositionSlice(BaseModel):
    household_type: str
    household_count: int
    share_ratio: float


class ResidenceRegionItem(BaseModel):
    region_name: str
    share_ratio: float
    estimated_customers: int


class SalesHeatmapCell(BaseModel):
    dow_label: str
    hour_band: str
    sales_index: int


class StoreReportItem(BaseModel):
    report_id: str
    title: str
    period: str
    generated_at: str
    status: str


class EstimatedSalesSummary(BaseModel):
    monthly_estimated_sales: float
    weekly_estimated_sales: float
    weekend_ratio: float


class IndustryBusinessTrendPoint(BaseModel):
    period: str
    business_count: int


class IndustryBusinessAgeItem(BaseModel):
    bucket: str
    business_count: int


class IndustryAnalysis(BaseModel):
    business_count_trend: list[IndustryBusinessTrendPoint]
    business_age_5y: list[IndustryBusinessAgeItem]


class SalesMonthlyTrendPoint(BaseModel):
    period: str
    sales_count: float
    sales_amount: float


class SalesAnalysis(BaseModel):
    monthly_sales_trend: list[SalesMonthlyTrendPoint]
    monthly_average_sales: float


class PopulationTrendPoint(BaseModel):
    period: str
    floating_population: int
    residential_population: int
    worker_population: int


class IncomeConsumptionItem(BaseModel):
    segment: str
    estimated_customers: int
    sales_share_ratio: float


class PopulationAnalysis(BaseModel):
    population_trend: list[PopulationTrendPoint]
    income_consumption: list[IncomeConsumptionItem]


class RegionalStatus(BaseModel):
    household_count: int
    apartment_household_count: int
    major_facilities_count: int
    transport_access_index: float


class CustomerCharacteristics(BaseModel):
    male_ratio: float
    female_ratio: float
    new_customer_ratio: float | None = None
    regular_customer_ratio: float | None = None
    top_age_group: str | None = None
    top_visit_time: str | None = None


class MarketIntelligenceResponse(BaseModel):
    radius_km: float
    category_sales_pie: list[TradeAreaSalesSlice]
    competitors: list[CompetitorInsightItem]
    residential_population_radar: list[ResidentialPopulationRadarItem]
    household_composition_pie: list[HouseholdCompositionSlice]
    estimated_residence_regions: list[ResidenceRegionItem]
    estimated_sales_summary: EstimatedSalesSummary
    sales_heatmap: list[SalesHeatmapCell]
    store_reports: list[StoreReportItem]
    floating_population_trend: list[FloatingPopulationTrendPoint]
    floating_population_analysis: str
    data_sources: list[str]
    industry_analysis: IndustryAnalysis
    sales_analysis: SalesAnalysis
    population_analysis: PopulationAnalysis
    regional_status: RegionalStatus
    customer_characteristics: CustomerCharacteristics
    explainability: ExplainabilityPayload | None = None


class MarketInsightItem(BaseModel):
    title: str
    description: str
    impact: Literal["high", "medium", "low"] = "medium"


class MarketRiskWarningItem(BaseModel):
    title: str
    description: str
    mitigation: str


class MarketActionItem(BaseModel):
    priority: int
    title: str
    action: str
    expected_effect: str


class BranchScoreboardItem(BaseModel):
    store_id: str
    store_name: str
    growth_rate: str
    risk_level: Literal["high", "medium", "low"] = "medium"
    summary: str


class MarketInsightsResponse(BaseModel):
    executive_summary: str
    key_insights: list[MarketInsightItem]
    risk_warnings: list[MarketRiskWarningItem]
    action_plan: list[MarketActionItem]
    branch_scoreboard: list[BranchScoreboardItem]
    report_markdown: str
    evidence_refs: list[str]
    audience: Literal["store_owner", "hq_admin"] = "store_owner"
    source: Literal["ai"] = "ai"
    trace_id: str | None = None
    explainability: ExplainabilityPayload | None = None


class HQMarketInsightsResponse(BaseModel):
    summary: MarketInsightsResponse
    branches: list[BranchScoreboardItem]
    explainability: ExplainabilityPayload | None = None
