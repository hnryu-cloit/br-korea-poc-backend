from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Literal

from app.core.ttl_cache import TTLMemoryCache
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
    IncomeConsumptionItem,
    IndustryAnalysis,
    IndustryBusinessAgeItem,
    IndustryBusinessTrendPoint,
    HouseholdCompositionSlice,
    MarketIntelligenceResponse,
    MarketInsightsResponse,
    PopulationAnalysis,
    PopulationTrendPoint,
    RegionalStatus,
    ResidenceRegionItem,
    ResidentialPopulationRadarItem,
    SalesAnalysis,
    SalesHeatmapCell,
    SalesMonthlyTrendPoint,
    SalesTrendInsightChip,
    SalesTrendPoint,
    SalesTrendResponse,
    StoreProfileResponse,
    StoreReportItem,
    TelecomDiscountItem,
    TradeAreaSalesSlice,
    WeatherImpactResponse,
    CustomerCharacteristics,
    HQMarketInsightsResponse,
    BranchScoreboardItem,
)
from app.services.ai_client import AIServiceClient
from app.services.explainability_service import create_ready_payload

logger = logging.getLogger(__name__)


_MARKET_INSIGHTS_TTL_SEC = 60


class AnalyticsService:
    def __init__(
        self,
        repository: AnalyticsRepository,
        ai_client: AIServiceClient | None = None,
    ) -> None:
        self.repository = repository
        self.ai_client = ai_client
        self._market_insights_cache = TTLMemoryCache(max_size=128)
        self._market_insights_refreshing: set[str] = set()

    async def get_metrics(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> AnalyticsMetricsResponse:
        metrics_data = await self.repository.get_metrics(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        response_items = [AnalyticsMetric(**item) for item in metrics_data["items"]]
        return AnalyticsMetricsResponse(
            items=response_items,
            selected_period_total_sales=metrics_data.get("selected_period_total_sales"),
            explainability=create_ready_payload(
                trace_id=f"analytics-metrics-{store_id or 'all'}",
                actions=["지표 변동이 큰 항목 1개를 선택해 오늘 운영 액션으로 연결하세요."],
                evidence=[f"조회 지표 수: {len(response_items)}"],
            ),
        )

    async def get_weather_impact(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> WeatherImpactResponse:
        payload = await self.repository.get_weather_impact(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        response = WeatherImpactResponse(**payload)
        response.explainability = create_ready_payload(
            trace_id=f"analytics-weather-impact-{store_id or 'all'}",
            actions=["강수/기온 영향이 큰 기간에 맞춰 재고·인력 배치를 조정하세요."],
            evidence=[
                f"분석 기간: {response.date_from}~{response.date_to}",
                f"권역 수: {len(response.items)}",
            ],
        )
        return response

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

    def get_sales_trend(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        compare_mode: Literal["prev_week", "prev_month"] = "prev_month",
    ):
        data = self.repository.get_sales_trend(
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
            compare_mode=compare_mode,
        )
        response = SalesTrendResponse(
            headline=data["headline"],
            headline_trend=data["headline_trend"],
            compare_mode=data["compare_mode"],
            date_from=data["date_from"],
            date_to=data["date_to"],
            comparison_date_from=data["comparison_date_from"],
            comparison_date_to=data["comparison_date_to"],
            selected_period_total_sales=data["selected_period_total_sales"],
            comparison_period_total_sales=data["comparison_period_total_sales"],
            points=[SalesTrendPoint(**p) for p in data["points"]],
            insight_chips=[SalesTrendInsightChip(**c) for c in data["insight_chips"]],
            dow_points=[DowPoint(**d) for d in data["dow_points"]],
            hour_points=[HourPoint(**h) for h in data["hour_points"]],
        )
        response.explainability = create_ready_payload(
            trace_id=f"analytics-sales-trend-{store_id or 'all'}",
            actions=["하락 구간 요일/시간대에 맞춰 프로모션 또는 인력 운영을 조정하세요."],
            evidence=[
                f"추세 포인트 수: {len(response.points)}",
                f"인사이트 칩 수: {len(response.insight_chips)}",
            ],
        )
        return response

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
        try:
            data = self.repository.get_market_intelligence(
                store_id=store_id,
                gu=gu,
                dong=dong,
                industry=industry,
                year=year,
                quarter=quarter,
                radius_m=radius_m,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "get_market_intelligence failed (store_id=%s, gu=%s, dong=%s, industry=%s, year=%s, quarter=%s, radius_m=%s): %s",
                store_id,
                gu,
                dong,
                industry,
                year,
                quarter,
                radius_m,
                exc,
            )
            raise RuntimeError("상권 원천 실데이터 조회에 실패했습니다.") from exc
        response = MarketIntelligenceResponse(
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
            industry_analysis=IndustryAnalysis(
                business_count_trend=[
                    IndustryBusinessTrendPoint(**point)
                    for point in data.get("industry_analysis", {}).get("business_count_trend", [])
                ],
                business_age_5y=[
                    IndustryBusinessAgeItem(**point)
                    for point in data.get("industry_analysis", {}).get("business_age_5y", [])
                ],
            ),
            sales_analysis=SalesAnalysis(
                monthly_sales_trend=[
                    SalesMonthlyTrendPoint(**point)
                    for point in data.get("sales_analysis", {}).get("monthly_sales_trend", [])
                ],
                monthly_average_sales=float(
                    data.get("sales_analysis", {}).get("monthly_average_sales", 0)
                ),
            ),
            population_analysis=PopulationAnalysis(
                population_trend=[
                    PopulationTrendPoint(**point)
                    for point in data.get("population_analysis", {}).get("population_trend", [])
                ],
                income_consumption=[
                    IncomeConsumptionItem(**point)
                    for point in data.get("population_analysis", {}).get("income_consumption", [])
                ],
            ),
            regional_status=RegionalStatus(
                **data.get(
                    "regional_status",
                    {
                        "household_count": 0,
                        "apartment_household_count": 0,
                        "major_facilities_count": 0,
                        "transport_access_index": 0.0,
                    },
                )
            ),
            customer_characteristics=CustomerCharacteristics(
                **data.get(
                    "customer_characteristics",
                    {
                        "male_ratio": 0.0,
                        "female_ratio": 0.0,
                        "new_customer_ratio": None,
                        "regular_customer_ratio": None,
                        "top_age_group": None,
                        "top_visit_time": None,
                    },
                )
            ),
        )
        response.explainability = create_ready_payload(
            trace_id=f"analytics-market-intelligence-{store_id or 'all'}",
            actions=[
                "상위 경쟁점과 차별화할 상품/프로모션 1개를 이번 주 실행 계획에 반영하세요.",
                "유동인구 추세가 높은 시간대 중심으로 운영 우선순위를 재배치하세요.",
            ],
            evidence=[
                f"경쟁점 수: {len(response.competitors)}",
                f"카테고리 비중 항목 수: {len(response.category_sales_pie)}",
                f"데이터 소스 수: {len(response.data_sources)}",
            ],
        )
        return response

    async def get_market_insights(
        self,
        *,
        store_id: str | None = None,
        gu: str | None = None,
        dong: str | None = None,
        industry: str | None = None,
        year: int | None = None,
        quarter: str | None = None,
        radius_m: int | None = None,
    ) -> MarketInsightsResponse:
        if not self.ai_client:
            raise RuntimeError("상권 인사이트 생성용 AI 클라이언트가 설정되지 않았습니다.")

        market = self.get_market_intelligence(
            store_id=store_id,
            gu=gu,
            dong=dong,
            industry=industry,
            year=year,
            quarter=quarter,
            radius_m=radius_m,
        )
        store_profile = self.get_store_profile(store_id=store_id)
        sales_trend = self.get_sales_trend(store_id=store_id)
        scope = {
            "store_id": store_id,
            "gu": gu,
            "dong": dong,
            "industry": industry,
            "year": year,
            "quarter": quarter,
            "radius_m": radius_m,
        }
        cache_key = self._build_market_insights_cache_key("store_owner", scope)
        cached = self._market_insights_cache.get(cache_key)
        if isinstance(cached, dict):
            self._schedule_market_insights_refresh(
                cache_key=cache_key,
                audience="store_owner",
                scope=scope,
                market_data=self._build_market_payload(market, sales_trend),
                branch_snapshots=[],
                store_name=store_profile.store_nm if store_profile else None,
            )
            return self._to_market_insights_response(
                cached,
                audience="store_owner",
                trace_id=f"analytics-market-insights-{store_id or 'all'}",
            )

        ai_result = await self._generate_market_insights_or_raise(
            audience="store_owner",
            scope=scope,
            market_data=self._build_market_payload(market, sales_trend),
            branch_snapshots=[],
            store_name=store_profile.store_nm if store_profile else None,
        )
        self._market_insights_cache.set(
            cache_key,
            ai_result,
            ttl_sec=_MARKET_INSIGHTS_TTL_SEC,
        )
        return self._to_market_insights_response(
            ai_result,
            audience="store_owner",
            trace_id=f"analytics-market-insights-{store_id or 'all'}",
        )

    async def get_hq_market_insights(
        self,
        *,
        gu: str | None = None,
        dong: str | None = None,
        industry: str | None = None,
        year: int | None = None,
        quarter: str | None = None,
        radius_m: int | None = None,
        limit: int = 20,
    ) -> HQMarketInsightsResponse:
        if not self.ai_client:
            raise RuntimeError("상권 인사이트 생성용 AI 클라이언트가 설정되지 않았습니다.")

        store_rows = self.repository.list_store_candidates(limit=limit)
        branch_snapshots: list[dict] = []
        for row in store_rows:
            store_id = str(row.get("store_id") or "")
            store_name = str(row.get("store_name") or store_id)
            if not store_id:
                continue
            market = self.get_market_intelligence(
                store_id=store_id,
                gu=gu,
                dong=dong,
                industry=industry,
                year=year,
                quarter=quarter,
                radius_m=radius_m,
            )
            monthly = float(market.estimated_sales_summary.monthly_estimated_sales or 0)
            weekend_ratio = float(market.estimated_sales_summary.weekend_ratio or 0)
            risk_level = "high" if weekend_ratio < 30 else "medium" if weekend_ratio < 40 else "low"
            branch_snapshots.append(
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "monthly_estimated_sales": monthly,
                    "weekend_ratio": weekend_ratio,
                    "risk_level": risk_level,
                    "floating_population_analysis": market.floating_population_analysis,
                }
            )

        scope = {
            "gu": gu,
            "dong": dong,
            "industry": industry,
            "year": year,
            "quarter": quarter,
            "radius_m": radius_m,
            "limit": limit,
        }
        cache_key = self._build_market_insights_cache_key("hq_admin", scope)
        cached = self._market_insights_cache.get(cache_key)
        if isinstance(cached, dict):
            self._schedule_market_insights_refresh(
                cache_key=cache_key,
                audience="hq_admin",
                scope=scope,
                market_data={"branch_count": len(branch_snapshots)},
                branch_snapshots=branch_snapshots,
                store_name=None,
            )
            summary = self._to_market_insights_response(
                cached,
                audience="hq_admin",
                trace_id=f"analytics-hq-market-insights-{gu or dong or 'all'}",
            )
            branches = (
                summary.branch_scoreboard
                if summary.branch_scoreboard
                else [self._to_branch_score(item) for item in branch_snapshots]
            )
            return HQMarketInsightsResponse(
                summary=summary,
                branches=branches,
                explainability=create_ready_payload(
                    trace_id=f"analytics-hq-market-insights-{gu or dong or 'all'}",
                    actions=["지점별 위험도 상위 매장부터 운영 코칭 계획을 확정하세요."],
                    evidence=[f"지점 스코어보드 건수: {len(branches)}"],
                ),
            )

        ai_result = await self._generate_market_insights_or_raise(
            audience="hq_admin",
            scope=scope,
            market_data={"branch_count": len(branch_snapshots)},
            branch_snapshots=branch_snapshots,
            store_name=None,
        )
        self._market_insights_cache.set(
            cache_key,
            ai_result,
            ttl_sec=_MARKET_INSIGHTS_TTL_SEC,
        )
        summary = self._to_market_insights_response(
            ai_result,
            audience="hq_admin",
            trace_id=f"analytics-hq-market-insights-{gu or dong or 'all'}",
        )
        branches = (
            summary.branch_scoreboard
            if summary.branch_scoreboard
            else [self._to_branch_score(item) for item in branch_snapshots]
        )
        return HQMarketInsightsResponse(
            summary=summary,
            branches=branches,
            explainability=create_ready_payload(
                trace_id=f"analytics-hq-market-insights-{gu or dong or 'all'}",
                actions=["지점별 위험도 상위 매장부터 운영 코칭 계획을 확정하세요."],
                evidence=[f"지점 스코어보드 건수: {len(branches)}"],
            ),
        )

    @staticmethod
    def _build_market_payload(
        market: MarketIntelligenceResponse,
        sales_trend: SalesTrendResponse,
    ) -> dict:
        return {
            "estimated_sales_summary": market.estimated_sales_summary.model_dump(),
            "category_sales_pie": [item.model_dump() for item in market.category_sales_pie],
            "floating_population_trend": [item.model_dump() for item in market.floating_population_trend],
            "customer_characteristics": market.customer_characteristics.model_dump(),
            "regional_status": market.regional_status.model_dump(),
            "sales_headline": sales_trend.headline,
            "insight_chips": [item.model_dump() for item in sales_trend.insight_chips],
            "data_sources": market.data_sources,
        }

    @staticmethod
    def _to_branch_score(item: dict) -> BranchScoreboardItem:
        weekend_ratio = float(item.get("weekend_ratio") or 0)
        growth_rate = f"주말 비중 {weekend_ratio:.1f}%"
        risk_level = str(item.get("risk_level") or "medium")
        if risk_level not in {"high", "medium", "low"}:
            risk_level = "medium"
        return BranchScoreboardItem(
            store_id=str(item.get("store_id") or ""),
            store_name=str(item.get("store_name") or "-"),
            growth_rate=growth_rate,
            risk_level=risk_level,
            summary=str(item.get("floating_population_analysis") or "상권 변동 데이터를 확인해 주세요."),
        )

    @staticmethod
    def _build_market_insights_cache_key(audience: str, scope: dict) -> str:
        return f"market-insights:{audience}:{json.dumps(scope, ensure_ascii=False, sort_keys=True)}"

    async def _generate_market_insights_or_raise(
        self,
        *,
        audience: str,
        scope: dict,
        market_data: dict,
        branch_snapshots: list[dict],
        store_name: str | None,
    ) -> dict:
        if not self.ai_client:
            raise RuntimeError("상권 인사이트 생성용 AI 클라이언트가 설정되지 않았습니다.")
        ai_result = await self.ai_client.generate_market_insights(
            audience=audience,
            scope=scope,
            market_data=market_data,
            branch_snapshots=branch_snapshots,
            store_name=store_name,
        )
        if not isinstance(ai_result, dict):
            raise RuntimeError("상권 인사이트 생성에 실패했습니다.")
        return ai_result

    def _schedule_market_insights_refresh(
        self,
        *,
        cache_key: str,
        audience: str,
        scope: dict,
        market_data: dict,
        branch_snapshots: list[dict],
        store_name: str | None,
    ) -> None:
        if cache_key in self._market_insights_refreshing:
            return
        self._market_insights_refreshing.add(cache_key)
        asyncio.create_task(
            self._refresh_market_insights_cache(
                cache_key=cache_key,
                audience=audience,
                scope=scope,
                market_data=market_data,
                branch_snapshots=branch_snapshots,
                store_name=store_name,
            )
        )

    async def _refresh_market_insights_cache(
        self,
        *,
        cache_key: str,
        audience: str,
        scope: dict,
        market_data: dict,
        branch_snapshots: list[dict],
        store_name: str | None,
    ) -> None:
        try:
            ai_result = await self._generate_market_insights_or_raise(
                audience=audience,
                scope=scope,
                market_data=market_data,
                branch_snapshots=branch_snapshots,
                store_name=store_name,
            )
            self._market_insights_cache.set(
                cache_key,
                ai_result,
                ttl_sec=_MARKET_INSIGHTS_TTL_SEC,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("상권 인사이트 캐시 백그라운드 갱신 실패: %s", exc)
        finally:
            self._market_insights_refreshing.discard(cache_key)

    @staticmethod
    def _to_market_insights_response(
        payload: dict,
        *,
        audience: str,
        trace_id: str,
    ) -> MarketInsightsResponse:
        return MarketInsightsResponse(
            **{
                "executive_summary": str(payload.get("executive_summary") or ""),
                "key_insights": payload.get("key_insights") or [],
                "risk_warnings": payload.get("risk_warnings") or [],
                "action_plan": payload.get("action_plan") or [],
                "branch_scoreboard": payload.get("branch_scoreboard") or [],
                "report_markdown": str(payload.get("report_markdown") or ""),
                "evidence_refs": payload.get("evidence_refs") or [],
                "audience": "hq_admin" if audience == "hq_admin" else "store_owner",
                "source": "ai",
                "trace_id": payload.get("trace_id"),
                "explainability": create_ready_payload(
                    trace_id=trace_id,
                    actions=[str(action.get("action") or action.get("title") or "").strip() for action in (payload.get("action_plan") or []) if isinstance(action, dict)],
                    evidence=[
                        f"핵심 인사이트 수: {len(payload.get('key_insights') or [])}",
                        f"리스크 경고 수: {len(payload.get('risk_warnings') or [])}",
                        f"근거 레퍼런스 수: {len(payload.get('evidence_refs') or [])}",
                    ],
                ),
            }
        )

    def get_weekly_market_report_markdown(
        self,
        *,
        store_id: str | None = None,
        gu: str | None = None,
        dong: str | None = None,
        industry: str | None = None,
        year: int | None = None,
        quarter: str | None = None,
        radius_m: int | None = None,
    ) -> tuple[str, str]:
        store_profile = self.get_store_profile(store_id=store_id)
        customer_profile = self.get_customer_profile(store_id=store_id)
        sales_trend = self.get_sales_trend(store_id=store_id)
        market = self.get_market_intelligence(
            store_id=store_id,
            gu=gu,
            dong=dong,
            industry=industry,
            year=year,
            quarter=quarter,
            radius_m=radius_m,
        )

        now = datetime.utcnow()
        report_date = now.strftime("%Y-%m-%d")
        report_week = now.strftime("%Y-W%V")
        area_label = " ".join([part for part in [gu, dong] if part and part != "전체"]).strip()
        if not area_label:
            if store_profile is not None:
                area_label = f"{store_profile.sido} {store_profile.region}".strip()
            else:
                area_label = "기본 상권"
        title_area = area_label or "상권"
        filename = f"weekly_market_report_{report_week}_{title_area.replace(' ', '_')}.md"

        top_customer = (
            max(customer_profile.customer_segments, key=lambda item: item.count)
            if customer_profile.customer_segments
            else None
        )
        top_competitor = market.competitors[0] if market.competitors else None
        trend_line = sales_trend.headline

        lines: list[str] = []
        lines.append(f"# {title_area} 주간 상권 분석 리포트")
        lines.append("")
        lines.append(f"- 기준일: {report_date}")
        lines.append(f"- 분석 범위: 반경 {market.radius_km:.1f}km")
        lines.append(f"- 기간 태그: {report_week}")
        lines.append("")
        lines.append("## 1. 핵심 요약")
        lines.append(
            f"- 월 추정매출: {int(market.estimated_sales_summary.monthly_estimated_sales):,}원"
        )
        lines.append(
            f"- 주 추정매출: {int(market.estimated_sales_summary.weekly_estimated_sales):,}원"
        )
        lines.append(
            f"- 주말 매출 비중: {market.estimated_sales_summary.weekend_ratio:.1f}%"
        )
        lines.append(f"- 매출 트렌드 헤드라인: {trend_line}")
        if top_customer:
            lines.append(
                f"- 핵심 고객군: {top_customer.segment_nm} ({top_customer.count:,}건)"
            )
        if top_competitor:
            lines.append(
                f"- 최고 경쟁 지점: {top_competitor.store_name} ({top_competitor.distance_km:.2f}km, {top_competitor.trend_direction})"
            )
        lines.append("")
        lines.append("## 2. 업종 매출 구성")
        for item in market.category_sales_pie:
            lines.append(
                f"- {item.category}: {int(item.sales_amount):,}원 ({item.share_ratio:.1f}%)"
            )
        lines.append("")
        lines.append("## 3. 유동인구·매출 추세")
        for point in market.floating_population_trend:
            lines.append(
                f"- {point.month}: 유동인구 {point.floating_population:,}명 / 추정매출 {int(point.estimated_sales_amount):,}원"
            )
        lines.append("")
        lines.append("## 4. 상위 경쟁사 트렌드 (Top 5)")
        for competitor in market.competitors[:5]:
            recent_sales = (
                int(competitor.sales_trend[-1].sales_amount)
                if competitor.sales_trend
                else 0
            )
            lines.append(
                f"- {competitor.rank}. {competitor.store_name}: 최근 매출지표 {recent_sales:,} / 거리 {competitor.distance_km:.2f}km / 추세 {competitor.trend_direction}"
            )
        lines.append("")
        lines.append("## 5. 소진공 OpenAPI 실호출 상태")
        lines.append("| API 코드 | 제목 | 상태 | 생성일 |")
        lines.append("|---|---|---|---|")
        for report in market.store_reports:
            lines.append(
                f"| {report.report_id} | {report.title} | {report.status} | {report.generated_at} |"
            )
        lines.append("")
        lines.append("## 6. 운영 제언")
        if market.estimated_sales_summary.weekend_ratio >= 40:
            lines.append("- 주말 집중형 인력/재고 운영을 강화합니다.")
        else:
            lines.append("- 주중 점심 피크타임 중심으로 회전율 최적화를 우선합니다.")
        if top_customer and "20" in top_customer.segment_nm:
            lines.append("- 20대 타깃 SNS/디저트형 프로모션 비중을 높입니다.")
        if top_customer and "30" in top_customer.segment_nm:
            lines.append("- 30대 직장인/가구 수요를 반영한 점심+저녁 투트랙 구성을 권장합니다.")
        if market.competitors:
            lines.append("- 상위 경쟁사 3개 지점의 가격·행사·리뷰 변화를 주 1회 점검합니다.")
        lines.append("")
        lines.append("## 7. 데이터 출처")
        for source in market.data_sources:
            lines.append(f"- {source}")
        lines.append("")
        lines.append("> 본 리포트는 내부 데이터와 소진공 OpenAPI 실호출 결과를 결합해 자동 생성되었습니다.")
        return filename, "\n".join(lines)

    @staticmethod
    def _wrap_line_for_pdf(line: str, max_chars: int = 92) -> list[str]:
        if len(line) <= max_chars:
            return [line]
        chunks: list[str] = []
        current = line
        while len(current) > max_chars:
            split_at = current.rfind(" ", 0, max_chars)
            if split_at <= 0:
                split_at = max_chars
            chunks.append(current[:split_at].rstrip())
            current = current[split_at:].lstrip()
        if current:
            chunks.append(current)
        return chunks or [""]

    @classmethod
    def _markdown_to_pdf_lines(cls, markdown: str) -> list[str]:
        lines: list[str] = []
        for raw in markdown.splitlines():
            text = raw.strip()
            if raw.startswith("# "):
                text = raw[2:].strip().upper()
            elif raw.startswith("## "):
                text = raw[3:].strip()
            elif raw.startswith("> "):
                text = f"NOTE: {raw[2:].strip()}"
            elif raw.startswith("- "):
                text = f"- {raw[2:].strip()}"
            elif raw.startswith("|"):
                text = raw.replace("|", " | ").strip()
            elif set(raw.strip()) == {"-"}:
                continue
            wrapped = cls._wrap_line_for_pdf(text)
            lines.extend(wrapped)
        return lines

    @staticmethod
    def _pdf_escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    @classmethod
    def render_weekly_market_report_pdf(
        cls, markdown: str, filename: str
    ) -> tuple[str, bytes]:
        pdf_filename = filename.rsplit(".", 1)[0] + ".pdf"
        lines = cls._markdown_to_pdf_lines(markdown)
        if not lines:
            lines = ["(empty report)"]

        page_width = 595
        page_height = 842
        left_margin = 45
        top_margin = 800
        line_height = 14
        max_lines_per_page = 52

        pages: list[list[str]] = []
        current_page: list[str] = []
        for line in lines:
            if len(current_page) >= max_lines_per_page:
                pages.append(current_page)
                current_page = []
            current_page.append(line)
        if current_page:
            pages.append(current_page)

        obj_id = 1
        catalog_id = obj_id
        obj_id += 1
        pages_id = obj_id
        obj_id += 1
        page_entries: list[tuple[int, int]] = []
        for _ in pages:
            page_id = obj_id
            obj_id += 1
            content_id = obj_id
            obj_id += 1
            page_entries.append((page_id, content_id))
        font_id = obj_id

        objects: list[tuple[int, bytes]] = []
        kids_refs = " ".join(f"{page_id} 0 R" for page_id, _ in page_entries)
        objects.append(
            (
                catalog_id,
                f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("latin-1"),
            )
        )
        objects.append(
            (
                pages_id,
                f"<< /Type /Pages /Kids [{kids_refs}] /Count {len(page_entries)} >>".encode("latin-1"),
            )
        )

        for page_idx, page_lines in enumerate(pages):
            page_id, content_id = page_entries[page_idx]
            commands = ["BT", f"/F1 10 Tf", f"1 0 0 1 {left_margin} {top_margin} Tm"]
            for line_idx, line in enumerate(page_lines):
                if line_idx == 0:
                    commands.append(f"({cls._pdf_escape(line)}) Tj")
                else:
                    commands.append(f"0 -{line_height} Td ({cls._pdf_escape(line)}) Tj")
            commands.append("ET")
            content_stream = "\n".join(commands).encode("latin-1", errors="replace")
            content_obj = (
                f"<< /Length {len(content_stream)} >>\nstream\n".encode("latin-1")
                + content_stream
                + b"\nendstream"
            )
            page_obj = (
                f"<< /Type /Page /Parent {pages_id} 0 R "
                f"/MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_id} 0 R >> >> "
                f"/Contents {content_id} 0 R >>"
            ).encode("latin-1")
            objects.append((page_id, page_obj))
            objects.append((content_id, content_obj))

        objects.append(
            (
                font_id,
                b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
            )
        )
        objects.sort(key=lambda item: item[0])

        output = bytearray()
        output.extend(b"%PDF-1.4\n")
        offsets: list[int] = [0]
        for obj_num, body in objects:
            offsets.append(len(output))
            output.extend(f"{obj_num} 0 obj\n".encode("latin-1"))
            output.extend(body)
            output.extend(b"\nendobj\n")

        xref_pos = len(output)
        output.extend(f"xref\n0 {len(offsets)}\n".encode("latin-1"))
        output.extend(b"0000000000 65535 f \n")
        for offset in offsets[1:]:
            output.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
        output.extend(
            (
                "trailer\n"
                f"<< /Size {len(offsets)} /Root {catalog_id} 0 R >>\n"
                f"startxref\n{xref_pos}\n%%EOF\n"
            ).encode("latin-1")
        )
        return pdf_filename, bytes(output)
