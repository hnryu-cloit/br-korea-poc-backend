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
        return ProductionOverviewResponse(
            updated_at="14:03",
            production_lead_time_minutes=60,
            danger_count=danger_count,
            items=items,
        )

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
