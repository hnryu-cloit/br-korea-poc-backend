from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.repositories.production_repository import ProductionRepository
from app.schemas.production import (
    ProductionAlertsResponse,
    ProductionAlertItem,
    ProductionOverviewResponse,
    ProductionRegistrationRequest,
    ProductionRegistrationHistoryItem,
    ProductionRegistrationHistoryResponse,
    ProductionRegistrationSummaryResponse,
    ProductionRegistrationResponse,
    ProductionSummaryStat,
    ProductionOverviewAlert,
    GetProductionSkuListResponse,
    ProductionSkuItem,
    ProductionSkuDecision,
    Pagination,
)
from app.services.audit_service import AuditService
from app.services.ai_client import AIServiceClient

class ProductionService:
    def __init__(
        self, 
        repository: ProductionRepository, 
        ai_client: Optional[AIServiceClient] = None,
        audit_service: Optional[AuditService] = None
    ) -> None:
        self.repository = repository
        self.ai_client = ai_client
        self.audit_service = audit_service

    async def get_overview(self) -> ProductionOverviewResponse:
        items = await self.repository.list_items()
        
        danger_count = sum(1 for item in items if item["status"] == "danger")
        warning_count = sum(1 for item in items if item["status"] == "warning")
        safe_count = sum(1 for item in items if item["status"] == "safe")
        
        summary_stats = [
            ProductionSummaryStat(key="danger_count", label="품절 위험", value=f"{danger_count}개", tone="danger"),
            ProductionSummaryStat(key="warning_count", label="주의 필요", value=f"{warning_count}개", tone="primary"),
            ProductionSummaryStat(key="safe_count", label="안전 재고", value=f"{safe_count}개", tone="success"),
            ProductionSummaryStat(key="chance_loss_saving_total", label="찬스 로스 절감", value="23%", tone="default"),
        ]
        
        alerts = []
        for item in items:
            if item["status"] == "danger":
                alerts.append(
                    ProductionOverviewAlert(
                        id=f"alert-{item['sku_id']}",
                        type="inventory_risk",
                        severity="high",
                        title=f"긴급: {item['name']} 재고 소진 임박",
                        description=f"현재 {item['current']}개, 1시간 후 {item['forecast']}개 예상. 지금 생산하면 찬스 로스 감소 가능",
                        sku_id=item["sku_id"],
                    )
                )
            elif item["status"] == "warning":
                 alerts.append(
                    ProductionOverviewAlert(
                        id=f"alert-{item['sku_id']}",
                        type="speed_risk",
                        severity="medium",
                        title=f"{item['name']} 소진 속도 상승",
                        description=f"평소보다 빠른 판매 속도 감지. 추가 생산 검토를 권장합니다.",
                        sku_id=item["sku_id"],
                    )
                )

        return ProductionOverviewResponse(
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            refresh_interval_minutes=5,
            summary_stats=summary_stats,
            alerts=alerts,
        )

    async def get_sku_list(self, page: int = 1, page_size: int = 20) -> GetProductionSkuListResponse:
        raw_items = await self.repository.list_items()
        
        items = []
        for raw in raw_items:
            # Parse prod1/prod2 strings like "08:10 / 52개"
            def parse_prod(s: str):
                parts = s.split(" / ")
                time = parts[0] if len(parts) > 0 else "00:00"
                qty = int(parts[1].replace("개", "")) if len(parts) > 1 else 0
                return time, qty
            
            p1_time, p1_qty = parse_prod(raw["prod1"])
            p2_time, p2_qty = parse_prod(raw["prod2"])
            
            # Map risk level for decision
            risk_label = "정상"
            if raw["status"] == "danger":
                risk_label = "즉시생산"
            elif raw["status"] == "warning":
                risk_label = "주의"
            
            decision = ProductionSkuDecision(
                risk_level_label=risk_label,
                sales_velocity=1.1 if raw["status"] != "safe" else 0.9,
                tags=["속도↑"] if raw["status"] != "safe" else [],
                alert_message="추가 생산이 권장됩니다." if raw["status"] != "safe" else "현재 재고가 안정적입니다.",
                can_produce=True,
                predicted_stockout_time=raw["depletion_time"] if raw["depletion_time"] != "-" else None,
                suggested_production_qty=raw["recommended"],
                chance_loss_prevented_amount=raw["recommended"] * 1200 if raw["recommended"] > 0 else None,
            )
            
            item = ProductionSkuItem(
                sku_id=raw["sku_id"],
                sku_name=raw["name"],
                current_stock=raw["current"],
                forecast_stock_1h=raw["forecast"],
                avg_first_production_qty_4w=p1_qty,
                avg_first_production_time_4w=p1_time,
                avg_second_production_qty_4w=p2_qty,
                avg_second_production_time_4w=p2_time,
                status=raw["status"],
                chance_loss_saving_pct=15 if raw["status"] == "danger" else 5 if raw["status"] == "warning" else 0,
                recommended_production_qty=raw["recommended"],
                chance_loss_basis_text="1시간 후 재고 예측 및 4주 평균 손실률 기준",
                decision=decision,
                depletion_eta_minutes=60 if raw["status"] != "safe" else None,
            )
            items.append(item)
            
        pagination = Pagination(
            page=page,
            page_size=page_size,
            total_items=len(items),
            total_pages=1,
        )
        
        return GetProductionSkuListResponse(items=items, pagination=pagination)

    async def get_alerts(self) -> ProductionAlertsResponse:
        items = await self.repository.list_items()
        alerts = [
            ProductionAlertItem(
                sku_id=item["sku_id"],
                name=item["name"],
                current=item["current"],
                forecast=item["forecast"],
                depletion_time=item["depletion_time"],
                recommended=item["recommended"],
                prod1=item["prod1"],
                prod2=item["prod2"],
                severity=item["status"],
                push_title=f"{item['name']} 생산이 필요합니다",
                push_message=(
                    f"현재고 {item['current']}개, 1시간 후 예상 {item['forecast']}개입니다. "
                    f"{item['depletion_time']} 전 소진 가능성이 있어 {item['recommended']}개 생산을 권장합니다."
                ),
                target_roles=["store_owner", "store_operator"],
            )
            for item in items
            if item["status"] in {"danger", "warning"}
        ]
        return ProductionAlertsResponse(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            lead_time_minutes=60,
            alerts=alerts,
        )

    async def register_production(self, payload: ProductionRegistrationRequest) -> ProductionRegistrationResponse:
        await self.repository.save_registration(payload.model_dump())
        response = ProductionRegistrationResponse(
            sku_id=payload.sku_id,
            qty=payload.qty,
            registered_by=payload.registered_by,
            feedback_type="chance_loss_reduced",
            feedback_message="재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
            store_id=payload.store_id,
        )
        if self.audit_service:
            await self.audit_service.record(
                domain="production",
                event_type="production_registered",
                actor_role=payload.registered_by,
                route="api",
                outcome="success",
                message=f"{payload.sku_id} 생산 등록을 저장했습니다.",
                metadata={"sku_id": payload.sku_id, "qty": payload.qty, "feedback_type": response.feedback_type},
            )
        return response

    async def list_registration_history(
        self,
        limit: int = 20,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> ProductionRegistrationHistoryResponse:
        items = await self.repository.list_registration_history(
            limit=limit,
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
        return ProductionRegistrationHistoryResponse(
            items=[ProductionRegistrationHistoryItem(**item) for item in items],
            total=len(items),
            filtered_store_id=store_id,
            filtered_date_from=date_from,
            filtered_date_to=date_to,
        )

    async def get_registration_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> ProductionRegistrationSummaryResponse:
        summary = await self.repository.get_registration_summary(
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
        latest = summary.get("latest")
        return ProductionRegistrationSummaryResponse(
            total=int(summary["total"]),
            latest=ProductionRegistrationHistoryItem(**latest) if latest else None,
            total_registered_qty=int(summary["total_registered_qty"]),
            recent_registered_by=list(summary["recent_registered_by"]),
            recent_registration_count_7d=int(summary["recent_registration_count_7d"]),
            recent_registered_qty_7d=int(summary["recent_registered_qty_7d"]),
            affected_sku_count=int(summary["affected_sku_count"]),
            summary_status=str(summary["summary_status"]),
            filtered_store_id=summary.get("filtered_store_id"),
            filtered_date_from=summary.get("filtered_date_from"),
            filtered_date_to=summary.get("filtered_date_to"),
        )

    async def _get_raw_simulation_data(self, store_id: str, target_date: str) -> dict:
        """AI 서버로 보낼 시연용 데이터를 추출합니다. 
        현재 백엔드 DB 연동 전이므로, 빈 배열을 반환하여 AI 서버의 기본 Mock 로직을 타게 합니다."""
        return {
            "inventory_data": [],
            "production_data": [],
            "sales_data": [],
            "store_production_data": []
        }

    async def get_dashboard_summary(self, store_id: str, target_date: str) -> dict:
        """AI 서버로부터 매장 대시보드 요약 정보를 가져옵니다."""
        if not self.ai_client:
            raise ValueError("AI_SERVICE_URL이 설정되지 않았습니다.")

        raw_data = await self._get_raw_simulation_data(store_id=store_id, target_date=target_date)

        result = await self.ai_client.get_home_dashboard(
            inventory_data=raw_data["inventory_data"],
            production_data=raw_data["production_data"],
            sales_data=raw_data["sales_data"],
            store_production_data=raw_data["store_production_data"]
        )
        return result

    async def get_home_overview(self, store_id: str, target_date: str) -> dict:
        """프론트엔드 홈 화면을 위한 통합 대시보드 조회"""
        return await self.get_dashboard_summary(store_id, target_date)

    async def run_simulation(self, payload: dict) -> dict:
        """AI 서버에 생산 시뮬레이션을 요청합니다."""
        if not self.ai_client:
            raise ValueError("AI_SERVICE_URL이 설정되지 않았습니다.")

        store_id = payload.get("store_id")
        simulation_date = payload.get("simulation_date")

        raw_data = await self._get_raw_simulation_data(store_id=store_id, target_date=simulation_date)

        result = await self.ai_client.run_production_simulation(
            payload=payload,
            inventory_data=raw_data["inventory_data"],
            production_data=raw_data["production_data"],
            sales_data=raw_data["sales_data"],
            store_production_data=raw_data["store_production_data"]
        )
        return result
