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
    ProductionSimulationRequest,
    ProductionSimulationResponse,
    SimulationChartPoint,
    SimulationSummaryMetrics,
)
from app.services.audit_service import AuditService


class ProductionService:
    def __init__(
        self,
        repository: ProductionRepository,
        audit_service: Optional[AuditService] = None,
        ai_client=None,
    ) -> None:
        self.repository = repository
        self.audit_service = audit_service
        self.ai_client = ai_client

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

    async def run_simulation(self, payload: ProductionSimulationRequest) -> ProductionSimulationResponse:
        inventory_data, production_data, sales_data = await self.repository.fetch_simulation_data(
            store_id=payload.store_id,
            item_id=payload.item_id,
            simulation_date=payload.simulation_date,
        )

        if self.ai_client:
            result = await self.ai_client.run_simulation(
                store_id=payload.store_id,
                item_id=payload.item_id,
                simulation_date=payload.simulation_date,
                lead_time_hour=payload.lead_time_hour,
                margin_rate=payload.margin_rate,
                inventory_data=inventory_data,
                production_data=production_data,
                sales_data=sales_data,
            )
            if result:
                return ProductionSimulationResponse(**result)

        return self._build_repository_simulation(
            payload=payload,
            inventory_data=inventory_data,
            production_data=production_data,
            sales_data=sales_data,
        )

    @staticmethod
    def _build_repository_simulation(
        payload: ProductionSimulationRequest,
        inventory_data: list[dict],
        production_data: list[dict],
        sales_data: list[dict],
    ) -> ProductionSimulationResponse:
        inventory_rows = [
            row for row in inventory_data
            if str(row.get("ITEM_CD") or "") == payload.item_id
        ]
        production_rows = [
            row for row in production_data
            if str(row.get("ITEM_CD") or "") == payload.item_id
        ]
        sales_rows = [
            row for row in sales_data
            if str(row.get("ITEM_CD") or "") == payload.item_id
        ]

        current_stock = sum(int(float(row.get("STOCK_QTY") or 0)) for row in inventory_rows)
        sales_qty = sum(int(float(row.get("SALE_QTY") or 0)) for row in sales_rows)
        production_qty = sum(int(float(row.get("PROD_QTY") or 0)) for row in production_rows)
        actual_end_stock = max(current_stock - sales_qty, 0)
        ai_guided_end_stock = max(current_stock + production_qty - sales_qty, 0)
        additional_sales_qty = max(min(production_qty, sales_qty) - max(current_stock - sales_qty, 0), 0)
        additional_profit_amt = int(round(additional_sales_qty * 1500 * payload.margin_rate))
        additional_waste_qty = float(max(ai_guided_end_stock - actual_end_stock, 0))
        additional_waste_cost = int(round(additional_waste_qty * 700))
        net_profit_change = additional_profit_amt - additional_waste_cost

        timeline = []
        if production_qty > 0:
            timeline.append(f"[생산 데이터] 누적 생산 {production_qty}개 반영")
        if sales_qty > 0:
            timeline.append(f"[판매 데이터] 누적 판매 {sales_qty}개 반영")

        chart_points = []
        for hour in ("08:00", "12:00", "16:00", "20:00"):
            chart_points.append(
                SimulationChartPoint(
                    time=hour,
                    actual_stock=float(actual_end_stock),
                    ai_guided_stock=float(ai_guided_end_stock),
                )
            )

        return ProductionSimulationResponse(
            metadata={
                "store_id": payload.store_id,
                "item_id": payload.item_id,
                "date": payload.simulation_date,
                "source": "repository",
                "inventory_rows": len(inventory_rows),
                "production_rows": len(production_rows),
                "sales_rows": len(sales_rows),
            },
            summary_metrics=SimulationSummaryMetrics(
                additional_sales_qty=float(additional_sales_qty),
                additional_profit_amt=additional_profit_amt,
                additional_waste_qty=additional_waste_qty,
                additional_waste_cost=additional_waste_cost,
                net_profit_change=net_profit_change,
                performance_status="POSITIVE" if net_profit_change >= 0 else "NEGATIVE",
                chance_loss_reduction=float(max(additional_profit_amt, 0)),
            ),
            time_series_data=chart_points,
            actions_timeline=timeline,
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
