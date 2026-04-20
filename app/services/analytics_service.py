from __future__ import annotations

from app.repositories.analytics_repository import AnalyticsRepository
from app.schemas.analytics import (
    AnalyticsMetric,
    AnalyticsMetricsResponse,
    CompetitorInsightItem,
    CompetitorPaymentDemographic,
    CompetitorTrendPoint,
    CustomerProfileResponse,
    CustomerSegmentItem,
    DowPoint,
    EstimatedSalesSummary,
    FloatingPopulationTrendPoint,
    HourPoint,
    HouseholdCompositionSlice,
    MarketIntelligenceResponse,
    ResidenceRegionItem,
    ResidentialPopulationRadarItem,
    SalesHeatmapCell,
    SalesTrendInsightChip,
    SalesTrendPoint,
    SalesTrendResponse,
    StoreProfileResponse,
    StoreReportItem,
    TelecomDiscountItem,
    TradeAreaSalesSlice,
    WeatherImpactResponse,
)


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository) -> None:
        self.repository = repository

    async def get_metrics(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> AnalyticsMetricsResponse:
        items = await self.repository.get_metrics(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        return AnalyticsMetricsResponse(items=[AnalyticsMetric(**item) for item in items])

    async def get_weather_impact(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> WeatherImpactResponse:
        payload = await self.repository.get_weather_impact(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        return WeatherImpactResponse(**payload)

    def get_store_profile(self, store_id: str | None = None):
        row = self.repository.get_store_profile(store_id=store_id)
        if row is None:
            return None
        return StoreProfileResponse(
            store_cd=row["masked_stor_cd"] or "",
            store_nm=row["maked_stor_nm"] or "",
            sido=row["sido"] or "",
            region=row["region"] or "",
            store_type=row["store_type"] or "",
            area_pyeong=int(row["store_area_pyeong"] or 0),
            business_type=row["business_type"] or "",
            peer_count=row["peer_count"],
            actual_sales_amt=float(row["actual_sales_amt"] or 0),
        )

    def get_customer_profile(self, store_id: str | None = None):
        data = self.repository.get_customer_profile(store_id=store_id)
        return CustomerProfileResponse(
            customer_segments=[CustomerSegmentItem(**s) for s in data["customer_segments"]],
            telecom_discounts=[TelecomDiscountItem(**t) for t in data["telecom_discounts"]],
        )

    def get_sales_trend(self, store_id: str | None = None):
        data = self.repository.get_sales_trend(store_id=store_id)
        return SalesTrendResponse(
            headline=data["headline"],
            headline_trend=data["headline_trend"],
            points=[SalesTrendPoint(**p) for p in data["points"]],
            insight_chips=[SalesTrendInsightChip(**c) for c in data["insight_chips"]],
            dow_points=[DowPoint(**d) for d in data["dow_points"]],
            hour_points=[HourPoint(**h) for h in data["hour_points"]],
        )

    def get_market_intelligence(
        self,
        store_id: str | None = None,
        gu: str | None = None,
        dong: str | None = None,
        industry: str | None = None,
        year: int | None = None,
        quarter: str | None = None,
        radius_m: int | None = None,
    ) -> MarketIntelligenceResponse:
        data = self.repository.get_market_intelligence(
            store_id=store_id,
            gu=gu,
            dong=dong,
            industry=industry,
            year=year,
            quarter=quarter,
            radius_m=radius_m,
        )
        return MarketIntelligenceResponse(
            radius_km=float(data.get("radius_km", 3.0)),
            category_sales_pie=[TradeAreaSalesSlice(**item) for item in data.get("category_sales_pie", [])],
            competitors=[
                CompetitorInsightItem(
                    rank=int(item.get("rank", 0)),
                    brand_name=str(item.get("brand_name", "")),
                    store_name=str(item.get("store_name", "")),
                    distance_km=float(item.get("distance_km", 0)),
                    trend_direction=str(item.get("trend_direction", "flat")),
                    sales_trend=[CompetitorTrendPoint(**point) for point in item.get("sales_trend", [])],
                    payment_demographics=[
                        CompetitorPaymentDemographic(**point)
                        for point in item.get("payment_demographics", [])
                    ],
                )
                for item in data.get("competitors", [])
            ],
            residential_population_radar=[
                ResidentialPopulationRadarItem(**point)
                for point in data.get("residential_population_radar", [])
            ],
            household_composition_pie=[
                HouseholdCompositionSlice(**item)
                for item in data.get("household_composition_pie", [])
            ],
            estimated_residence_regions=[
                ResidenceRegionItem(**item)
                for item in data.get("estimated_residence_regions", [])
            ],
            estimated_sales_summary=EstimatedSalesSummary(
                **data.get(
                    "estimated_sales_summary",
                    {
                        "monthly_estimated_sales": 0,
                        "weekly_estimated_sales": 0,
                        "weekend_ratio": 0,
                    },
                )
            ),
            sales_heatmap=[
                SalesHeatmapCell(**item)
                for item in data.get("sales_heatmap", [])
            ],
            store_reports=[
                StoreReportItem(**item)
                for item in data.get("store_reports", [])
            ],
            floating_population_trend=[
                FloatingPopulationTrendPoint(**point)
                for point in data.get("floating_population_trend", [])
            ],
            floating_population_analysis=str(data.get("floating_population_analysis", "")),
            data_sources=[str(source) for source in data.get("data_sources", [])],
        )
