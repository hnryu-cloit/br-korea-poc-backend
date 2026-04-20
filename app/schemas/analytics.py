from __future__ import annotations

from pydantic import BaseModel


class AnalyticsMetric(BaseModel):
    label: str
    value: str
    change: str
    trend: str
    detail: str


class AnalyticsMetricsResponse(BaseModel):
    items: list[AnalyticsMetric]


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


class SalesTrendPoint(BaseModel):
    day: int
    this_month: float | None = None
    last_month: float | None = None
    projection: float | None = None


class SalesTrendInsightChip(BaseModel):
    label: str
    value: str
    trend: str


class DowPoint(BaseModel):
    dow: int  # 0=월 ~ 6=일
    label: str  # 월·화·수·목·금·토·일
    this_month_avg: float
    last_month_avg: float


class HourPoint(BaseModel):
    hour: int
    this_month_avg: float
    last_month_avg: float


class SalesTrendResponse(BaseModel):
    headline: str
    headline_trend: str
    points: list[SalesTrendPoint]
    insight_chips: list[SalesTrendInsightChip]
    dow_points: list[DowPoint]
    hour_points: list[HourPoint]


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
