from app.repositories.production_repository import ProductionRepository
from app.schemas.production import (
    ProductionOverviewResponse,
    ProductionRegistrationRequest,
    ProductionRegistrationResponse,
)


class ProductionService:
    def __init__(self, repository: ProductionRepository) -> None:
        self.repository = repository

    async def get_overview(self) -> ProductionOverviewResponse:
        items = await self.repository.list_items()
        danger_count = sum(1 for item in items if item["status"] == "danger")
        return ProductionOverviewResponse(
            updated_at="14:03",
            production_lead_time_minutes=60,
            danger_count=danger_count,
            items=items,
        )

    async def register_production(self, payload: ProductionRegistrationRequest) -> ProductionRegistrationResponse:
        await self.repository.save_registration(payload.model_dump())
        return ProductionRegistrationResponse(
            sku_id=payload.sku_id,
            qty=payload.qty,
            registered_by=payload.registered_by,
            feedback_type="chance_loss_reduced",
            feedback_message="재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
        )
